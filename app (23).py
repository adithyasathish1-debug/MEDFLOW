from __future__ import annotations

import csv
import io
import itertools
import math
import os
import random
import threading
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Any

from flask import Flask, Response, jsonify, render_template_string, request

# ============================================================
# MEDFLOW — Hospital Resource Management Simulator
# ============================================================
# Single-file Flask application.
# Run: python app.py
# Open: http://127.0.0.1:5000
#
# ============================================================

app = Flask(__name__)
app.config["JSON_SORT_KEYS"] = False

URGENCY_LEVELS = {1: "Critical", 2: "Urgent", 3: "Moderate", 4: "Low"}
URGENCY_WEIGHTS = [8, 22, 40, 30]
DEPARTMENTS = ["Emergency", "General", "Cardiology", "Surgery", "Pediatrics"]
FIRST_NAMES = ["Amara", "Rohan", "Elena", "Kofi", "Mei", "Diego", "Sana", "Liam", "Priya", "Noah", "Fatima", "Ivo", "Chen", "Zoe", "Omar", "Ana"]
LAST_INITIALS = list("ABCDEFGHJKLMNPQRSTUVWXYZ")

DEFAULT_CAPACITY = {
    "bed": 40,
    "icu": 8,
    "or": 4,
    "doctor": 12,
    "nurse": 20,
    "ambulance": 6,
}

PROFILES = {
    1: {"needs": {"bed": 1, "icu": 1, "doctor": 1, "nurse": 2, "ambulance": 1}, "duration": (6, 14), "or_chance": 0.35},
    2: {"needs": {"bed": 1, "doctor": 1, "nurse": 1, "ambulance": 1}, "duration": (4, 10), "or_chance": 0.15},
    3: {"needs": {"bed": 1, "doctor": 1, "nurse": 1}, "duration": (3, 7), "or_chance": 0.03},
    4: {"needs": {"bed": 1, "nurse": 1}, "duration": (1, 4), "or_chance": 0.0},
}

STRATEGY_LABELS = {
    "urgency_only": "Urgency Only",
    "urgency_wait": "Urgency + Wait Time",
    "utilization_aware": "Urgency + Wait + Utilization",
    "optimization_engine": "Optimization Engine",
}

patient_counter = itertools.count(1)

def parse_bool(value, default=False):
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        text = value.strip().lower()
        if text in {"1", "true", "yes", "on", "enabled"}: return True
        if text in {"0", "false", "no", "off", "disabled"}: return False
    return default


@dataclass
class Patient:
    id: int
    name: str
    urgency: int
    department: str
    arrival_time: int
    needs: dict[str, int]
    duration: int
    via_ambulance: bool = False
    status: str = "waiting"
    assigned: dict[str, int] = field(default_factory=dict)
    start_time: int | None = None
    end_time: int | None = None
    wait_time: int = 0

    @property
    def urgency_label(self):
        return URGENCY_LEVELS[self.urgency]

    def to_dict(self):
        return {
            "id": self.id,
            "name": self.name,
            "urgency": self.urgency,
            "urgency_label": self.urgency_label,
            "department": self.department,
            "arrival_time": self.arrival_time,
            "needs": self.needs,
            "duration": self.duration,
            "via_ambulance": self.via_ambulance,
            "status": self.status,
            "assigned": self.assigned,
            "start_time": self.start_time,
            "end_time": self.end_time,
            "wait_time": self.wait_time,
        }


@dataclass
class ResourcePool:
    name: str
    total: int
    available: int | None = None
    down: int = 0
    allocated: int = 0

    def __post_init__(self):
        if self.available is None:
            self.available = self.total
        self.available = max(0, min(int(self.available), self.total))
        self.allocated = max(0, self.total - self.available)

    @property
    def operational(self):
        return max(0, self.total - self.down)

    @property
    def in_use(self):
        # Allocated units remain allocated even when some physical units fail.
        # This prevents failures from silently releasing patients' resources.
        return self.allocated

    @property
    def utilization(self):
        # Can exceed 100% when a failure removes capacity already in use. That
        # is intentional: it exposes overload instead of hiding it.
        return round((self.in_use / self.operational) * 100, 1) if self.operational else 0.0

    @property
    def overload(self):
        return max(0, self.in_use - self.operational)

    def _sync_available(self):
        self.available = max(0, self.operational - self.allocated)

    def can_allocate(self, qty):
        return int(qty) >= 0 and qty <= self.available

    def allocate(self, qty):
        qty = int(qty)
        if not self.can_allocate(qty):
            raise ValueError(f"Capacity violation: {self.name}")
        self.allocated += qty
        self._sync_available()

    def release(self, qty):
        qty = max(0, int(qty))
        self.allocated = max(0, self.allocated - qty)
        self._sync_available()

    def set_down(self, target_down):
        self.down = max(0, min(int(target_down), self.total))
        self._sync_available()

    def to_dict(self):
        return {
            "name": self.name,
            "total": self.total,
            "operational": self.operational,
            "available": self.available,
            "in_use": self.in_use,
            "allocated": self.allocated,
            "down": self.down,
            "overload": self.overload,
            "utilization": self.utilization,
        }


class Hospital:
    def __init__(self, capacity=None, strategy="urgency_wait", seed=7):
        cap = dict(DEFAULT_CAPACITY)
        cap.update(capacity or {})
        self.resources = {k: ResourcePool(k, int(v)) for k, v in cap.items()}
        self.strategy = strategy if strategy in STRATEGY_LABELS else "urgency_wait"
        self.seed = int(seed)
        self.rng = random.Random(self.seed)
        self.tick_count = 0
        self.waiting: list[Patient] = []
        self.in_treatment: list[Patient] = []
        self.discharged: list[Patient] = []
        self.events = deque(maxlen=100)
        self.history = []
        self.arrival_rate = 1.4
        self.ambulance_rate = 0.35
        self.surge = False
        self.surge_multiplier = 3.0
        self.staff_shortage = False
        self.shortage_pct = 0.40
        self.auto_failures = False
        self.failure_chance = 0.06
        self.icu_constrained = True
        self.conflicts = 0
        self.allocations = 0
        self.arrivals_total = 0
        self.arrivals_last_tick = 0
        self.admissions_last_tick = 0
        self.discharges_last_tick = 0
        self.blocked_last_tick = 0
        self.started_at = time.time()
        self._apply_staff_shortage()

    def log(self, message, kind="info"):
        self.events.appendleft({"tick": self.tick_count, "message": message, "kind": kind, "ts": time.time()})

    def _apply_staff_shortage(self):
        for name in ("doctor", "nurse"):
            pool = self.resources[name]
            pool.set_down(int(pool.total * self.shortage_pct) if self.staff_shortage else 0)

    def set_strategy(self, strategy):
        if strategy not in STRATEGY_LABELS:
            raise ValueError("Unknown scheduling strategy")
        self.strategy = strategy
        self.log(f"Scheduling strategy changed to {STRATEGY_LABELS[strategy]}.", "config")

    def configure(self, data):
        if "surge" in data:
            self.surge = parse_bool(data["surge"], self.surge)
            self.log("Emergency patient surge activated." if self.surge else "Emergency surge ended.", "surge" if self.surge else "info")
        if "staff_shortage" in data:
            self.staff_shortage = parse_bool(data["staff_shortage"], self.staff_shortage)
            self._apply_staff_shortage()
            self.log("Staff shortage activated." if self.staff_shortage else "Staffing restored.", "shortage" if self.staff_shortage else "info")
        if "auto_failures" in data:
            self.auto_failures = parse_bool(data["auto_failures"], self.auto_failures)
        if "icu_constrained" in data:
            self.icu_constrained = parse_bool(data["icu_constrained"], self.icu_constrained)
        for key in ("arrival_rate", "ambulance_rate", "surge_multiplier", "shortage_pct", "failure_chance"):
            if key in data:
                try:
                    value = float(data[key])
                    if key == "shortage_pct": value = max(0, min(value, 0.9))
                    if key == "failure_chance": value = max(0, min(value, 1))
                    if key in ("arrival_rate", "ambulance_rate", "surge_multiplier"): value = max(0, value)
                    setattr(self, key, value)
                except (TypeError, ValueError):
                    pass
        return self.state()

    def trigger_failure(self, resource=None):
        if resource is None or resource == "":
            resource = self.rng.choice(list(self.resources))
        if resource not in self.resources:
            raise ValueError(f"Unknown resource '{resource}'")
        pool = self.resources[resource]
        extra = max(1, int(pool.total * self.rng.uniform(0.15, 0.35)))
        pool.set_down(min(pool.total, pool.down + extra))
        self.log(f"Failure: {resource} — {extra} additional unit(s) offline.", "failure")

    def resolve_failures(self):
        for name, pool in self.resources.items():
            if name in ("doctor", "nurse") and self.staff_shortage:
                pool.set_down(int(pool.total * self.shortage_pct))
            else:
                pool.set_down(0)
        self.log("Recoverable resource failures cleared.", "info")

    def _poisson(self, rate):
        """Exact Knuth Poisson draw for the arrival process (no NumPy required)."""
        if rate <= 0:
            return 0
        # For this simulator rates are intentionally small, so the classic
        # product method is stable, deterministic and easy to audit.
        limit = math.exp(-rate)
        k, product = 0, 1.0
        while product > limit:
            k += 1
            product *= self.rng.random()
        return k - 1

    def _make_patient(self, ambulance=False, urgency=None):
        urgency = urgency or self.rng.choices([1, 2, 3, 4], weights=URGENCY_WEIGHTS)[0]
        profile = PROFILES[urgency]
        needs = dict(profile["needs"])
        if self.rng.random() < profile["or_chance"]:
            needs["or"] = 1
        # Ambulance availability is consumed only for patients arriving by ambulance.
        if not ambulance:
            needs.pop("ambulance", None)
        name = f"{self.rng.choice(FIRST_NAMES)} {self.rng.choice(LAST_INITIALS)}."
        dept = "Emergency" if ambulance or urgency <= 2 else self.rng.choice(DEPARTMENTS)
        return Patient(next(patient_counter), name, urgency, dept, self.tick_count,
                       needs, self.rng.randint(*profile["duration"]), ambulance)

    def _spawn_arrivals(self):
        mult = self.surge_multiplier if self.surge else 1.0
        walkins = self._poisson(self.arrival_rate * mult)
        ambulances = self._poisson(self.ambulance_rate * mult)
        for _ in range(walkins): self.waiting.append(self._make_patient(False))
        for _ in range(ambulances): self.waiting.append(self._make_patient(True, self.rng.choice([1, 1, 2, 2])))
        arrivals = walkins + ambulances
        self.arrivals_total += arrivals
        self.arrivals_last_tick = arrivals
        if arrivals:
            self.log(f"{walkins + ambulances} arrival(s): {ambulances} ambulance, {walkins} walk-in.", "arrival")

    def _discharge(self):
        keep = []
        discharged_now = 0
        for p in self.in_treatment:
            if p.end_time is not None and self.tick_count >= p.end_time:
                for r, qty in p.assigned.items(): self.resources[r].release(qty)
                p.status = "discharged"
                self.discharged.append(p)
                discharged_now += 1
                self.log(f"Patient #{p.id} discharged from {p.department}.", "discharge")
            else:
                keep.append(p)
        self.in_treatment = keep
        self.discharges_last_tick = discharged_now

    def _optimization_components(self, p):
        """Explainable multi-objective admission model.

        The objective protects urgent patients and prevents starvation while
        charging the plan for scarce-resource consumption, fragmentation and
        reserve violations. It is a bounded operational heuristic, not a
        clinical decision rule or a proof of global optimality.
        """
        wait = max(0, self.tick_count - p.arrival_time)
        urgency_risk = {1: 120.0, 2: 72.0, 3: 30.0, 4: 10.0}[p.urgency]
        starvation = min(wait * 4.0, 42.0)
        scarcity_cost = 0.0
        fragmentation = 0.0
        reserve_penalty = 0.0
        if not self._can_fit(p):
            return {"feasible": False, "benefit": -1e9, "urgency": urgency_risk,
                    "wait": starvation, "scarcity_cost": 999.0, "fragmentation": 999.0,
                    "reserve_penalty": 999.0}
        for name, qty in p.needs.items():
            pool = self.resources[name]
            util = pool.in_use / max(1, pool.operational)
            weight = 7.0 if name == "icu" else 5.0 if name == "or" else 2.0
            scarcity_cost += (util * util) * weight * qty
            remaining = pool.available - qty
            if remaining == 0:
                fragmentation += 3.5 * qty
            elif remaining == 1 and name in ("icu", "or"):
                fragmentation += 7.0 * qty
            if name == "icu" and p.urgency > 1 and remaining < 1:
                reserve_penalty += 22.0
            if name == "or" and p.urgency > 2 and remaining < 1:
                reserve_penalty += 8.0
        ambulance_bonus = 4.0 if p.via_ambulance and p.urgency <= 2 else 0.0
        fairness_bonus = min(wait * 0.8, 12.0)
        benefit = urgency_risk + starvation + fairness_bonus + ambulance_bonus - scarcity_cost - fragmentation - reserve_penalty
        return {"feasible": True, "benefit": round(benefit, 3), "urgency": urgency_risk,
                "wait": starvation, "scarcity_cost": round(scarcity_cost, 3),
                "fragmentation": round(fragmentation, 3), "reserve_penalty": round(reserve_penalty, 3),
                "fairness_bonus": round(fairness_bonus, 3), "ambulance_bonus": ambulance_bonus}

    def _optimization_score(self, p):
        c = self._optimization_components(p)
        return (-c["benefit"], p.arrival_time, p.id)

    def _score(self, p):
        wait = max(0, self.tick_count - p.arrival_time)
        if self.strategy == "optimization_engine":
            return self._optimization_score(p)
        if self.strategy == "urgency_only":
            return (p.urgency, p.arrival_time, p.id)
        # Lower score = earlier admission. Waiting time progressively offsets
        # urgency so a low-acuity patient cannot starve indefinitely.
        score = p.urgency * 100 - min(wait * 6.0, 38.0)
        if self.strategy == "utilization_aware":
            pressure = 0.0
            for name, qty in p.needs.items():
                pool = self.resources.get(name)
                if pool and pool.operational:
                    pressure += (pool.in_use / pool.operational) * (7.0 if name in ("icu", "or") else 2.0) * qty
            score += pressure
        return (score, p.arrival_time, p.id)

    def _can_fit(self, p):
        if not all(r in self.resources and self.resources[r].can_allocate(q) for r, q in p.needs.items()):
            return False
        # ICU conservation rule: preserve one operational ICU slot for critical arrivals
        # whenever the candidate is not critical and the unit would consume the final slot.
        if self.icu_constrained and p.urgency > 1 and p.needs.get("icu", 0):
            icu = self.resources.get("icu")
            if icu and icu.available - p.needs.get("icu", 0) < 1:
                return False
        return True

    def _allocate(self, p):
        if not self._can_fit(p):
            return False
        for r, q in p.needs.items(): self.resources[r].allocate(q)
        p.assigned = dict(p.needs)
        p.status = "in_treatment"
        p.start_time = self.tick_count
        p.end_time = self.tick_count + p.duration
        p.wait_time = self.tick_count - p.arrival_time
        self.in_treatment.append(p)
        self.allocations += 1
        return True

    def _schedule(self):
        ordered = sorted(self.waiting, key=self._score)
        remain = []
        admitted = 0
        blocked_this_tick = 0
        for p in ordered:
            if self._allocate(p):
                admitted += 1
            else:
                remain.append(p)
                blocked_this_tick += 1
        self.conflicts += blocked_this_tick
        self.blocked_last_tick = blocked_this_tick
        self.admissions_last_tick = admitted
        self.waiting = remain
        if admitted or blocked_this_tick:
            self.log(f"Scheduling pass: {admitted} admitted, {blocked_this_tick} blocked by current capacity.", "admit" if admitted else "warning")

    def _metrics(self):
        waits = [self.tick_count - p.arrival_time for p in self.waiting]
        util = {k: v.utilization for k, v in self.resources.items()}
        snap = {
            "tick": self.tick_count,
            "queue_len": len(self.waiting),
            "urgent_waiting": sum(p.urgency <= 2 for p in self.waiting),
            "in_treatment": len(self.in_treatment),
            "discharged_total": len(self.discharged),
            "avg_wait": round(sum(waits) / len(waits), 2) if waits else 0,
            "max_wait": max(waits, default=0),
            "avg_resource_utilization": round(sum(util.values()) / len(util), 1),
            "resource_utilization": util,
            "allocations": self.allocations,
            "arrivals_total": self.arrivals_total,
            "arrivals_this_tick": self.arrivals_last_tick,
            "admissions_this_tick": self.admissions_last_tick,
            "discharges_this_tick": self.discharges_last_tick,
            "blocked_this_tick": self.blocked_last_tick,
            "capacity_conflicts": self.conflicts,
        }
        self.history.append(snap)
        self.history = self.history[-240:]
        return snap

    def import_patients(self, rows):
        """Validate and add CSV patients to the waiting queue.
        Required: name, urgency, department. Optional: arrival_time, duration and resource quantities.
        """
        added, errors = 0, []
        allowed = set(DEFAULT_CAPACITY)
        for idx, row in enumerate(rows, start=2):
            try:
                name = str(row.get("name", "")).strip() or f"Imported {idx}"
                raw_u = str(row.get("urgency", "")).strip().lower()
                label_map = {v.lower(): k for k, v in URGENCY_LEVELS.items()}
                urgency = int(raw_u) if raw_u.isdigit() else label_map.get(raw_u)
                if urgency not in URGENCY_LEVELS:
                    raise ValueError("urgency must be 1-4 or Critical/Urgent/Moderate/Low")
                dept = str(row.get("department", "General")).strip() or "General"
                if dept not in DEPARTMENTS:
                    raise ValueError(f"department must be one of: {', '.join(DEPARTMENTS)}")
                arrival = int(float(row.get("arrival_time", self.tick_count)))
                duration = max(1, int(float(row.get("duration", sum(PROFILES[urgency]["duration"]) / 2))))
                needs = {}
                for resource in ("bed", "icu", "or", "doctor", "nurse", "ambulance"):
                    value = str(row.get(resource, "0")).strip()
                    qty = int(float(value)) if value else 0
                    if qty < 0 or qty > DEFAULT_CAPACITY[resource]:
                        raise ValueError(f"{resource} quantity is out of range")
                    if qty: needs[resource] = qty
                if not needs:
                    needs = dict(PROFILES[urgency]["needs"])
                    needs.pop("ambulance", None)
                via = bool(needs.get("ambulance", 0))
                self.waiting.append(Patient(next(patient_counter), name[:80], urgency, dept, arrival,
                                             needs, duration, via_ambulance=via))
                added += 1
            except Exception as exc:
                errors.append({"row": idx, "error": str(exc)})
        if added:
            self.log(f"Imported {added} patient(s) from CSV.", "import")
        return {"added": added, "errors": errors, "total_waiting": len(self.waiting)}

    def tick(self, n=1):
        for _ in range(max(1, min(int(n), 100))):
            self.tick_count += 1
            self.arrivals_last_tick = self.admissions_last_tick = self.discharges_last_tick = self.blocked_last_tick = 0
            if self.auto_failures and self.rng.random() < self.failure_chance:
                self.trigger_failure()
            self._spawn_arrivals()
            self._discharge()
            self._schedule()
            self._metrics()
        return self.state()

    def reset(self, seed=None, strategy=None):
        return Hospital(capacity=DEFAULT_CAPACITY, strategy=strategy or self.strategy, seed=self.seed if seed is None else int(seed))

    def analytics(self):
        """Operational analytics: bottlenecks, headroom, queue risk, forecast and actions."""
        resource_rows=[]
        for name,pool in self.resources.items():
            headroom = round((pool.available / pool.operational) * 100, 1) if pool.operational else 0
            resource_rows.append({"resource":name,"utilization":pool.utilization,"available":pool.available,"operational":pool.operational,"headroom_pct":headroom,"down":pool.down})
        resource_rows.sort(key=lambda x:(-x["utilization"], x["resource"]))
        waits=[max(0,self.tick_count-p.arrival_time) for p in self.waiting]
        urgent=[p for p in self.waiting if p.urgency<=2]
        by_urg={str(i):{"count":0,"avg_wait":0,"max_wait":0} for i in range(1,5)}
        for p in self.waiting:
            w=max(0,self.tick_count-p.arrival_time); row=by_urg[str(p.urgency)]; row["count"]+=1; row["avg_wait"]+=w; row["max_wait"]=max(row["max_wait"],w)
        for row in by_urg.values():
            if row["count"]: row["avg_wait"]=round(row["avg_wait"]/row["count"],2)
        mult=self.surge_multiplier if self.surge else 1.0
        expected_arrivals=round((self.arrival_rate+self.ambulance_rate)*mult,2)
        recent=self.history[-12:]
        avg_util=round(sum(x["avg_resource_utilization"] for x in recent)/len(recent),1) if recent else 0
        queue_growth=0
        if len(recent)>=2: queue_growth=round((recent[-1]["queue_len"]-recent[0]["queue_len"])/max(1,len(recent)-1),2)
        bottleneck=resource_rows[0]["resource"] if resource_rows else None
        actions=[]
        if urgent: actions.append(f"Protect {len(urgent)} urgent patient(s) from prolonged waiting.")
        if bottleneck and resource_rows[0]["utilization"]>=85: actions.append(f"{bottleneck.upper()} is under high pressure; preserve headroom before admitting non-urgent cases.")
        if queue_growth>0.5: actions.append("Queue is growing; consider increasing capacity or reducing arrival pressure.")
        if self.staff_shortage: actions.append("Restore staff capacity or redistribute workload across departments.")
        if self.auto_failures and any(r["down"] for r in resource_rows): actions.append("Review offline resources before relying on normal capacity assumptions.")
        if not actions: actions.append("Capacity is currently balanced; continue monitoring queue and utilization trends.")
        return {
            "bottlenecks":resource_rows[:6],
            "queue_risk":{"waiting":len(self.waiting),"urgent_waiting":len(urgent),"avg_wait":round(sum(waits)/len(waits),2) if waits else 0,"max_wait":max(waits,default=0),"growth_per_tick":queue_growth},
            "urgency_profile":by_urg,
            "forecast":{"expected_arrivals_next_tick":expected_arrivals,"expected_next_6_ticks":round(expected_arrivals*6,1),"current_avg_utilization":avg_util},
            "throughput":{"arrivals_total":self.arrivals_total,"discharged":len(self.discharged),"active":len(self.in_treatment),"waiting":len(self.waiting)},
            "recommendations":actions,
            "capacity_pressure":round(min(100, max(0, avg_util + len(urgent)*4 + max(queue_growth,0)*10)),1),
        }

    def department_analytics(self):
        rows = []
        for dept in DEPARTMENTS:
            waiting = [p for p in self.waiting if p.department == dept]
            active = [p for p in self.in_treatment if p.department == dept]
            discharged = [p for p in self.discharged if p.department == dept]
            waits = [max(0, self.tick_count - p.arrival_time) for p in waiting]
            rows.append({
                "department": dept,
                "waiting": len(waiting),
                "urgent_waiting": sum(p.urgency <= 2 for p in waiting),
                "in_treatment": len(active),
                "discharged": len(discharged),
                "avg_wait": round(sum(waits) / len(waits), 2) if waits else 0,
                "max_wait": max(waits, default=0),
            })
        return rows

    def forecast(self, horizon=6):
        horizon = max(1, min(int(horizon), 48))
        mult = self.surge_multiplier if self.surge else 1.0
        rate = (self.arrival_rate + self.ambulance_rate) * mult
        expected = [round(rate, 2) for _ in range(horizon)]
        return {
            "horizon": horizon,
            "expected_arrivals_per_tick": expected,
            "expected_total_arrivals": round(sum(expected), 2),
            "queue_now": len(self.waiting),
            "projected_queue_without_new_admissions": round(len(self.waiting) + sum(expected), 2),
            "method": "Poisson mean based on current configured walk-in + ambulance rates",
        }

    def state(self):
        wait_by_urgency = {str(i): sum(p.urgency == i for p in self.waiting) for i in range(1, 5)}
        dept_load = {d: 0 for d in DEPARTMENTS}
        for p in self.in_treatment: dept_load[p.department] = dept_load.get(p.department, 0) + 1
        return {
            "tick": self.tick_count,
            "seed": self.seed,
            "strategy": self.strategy,
            "strategy_label": STRATEGY_LABELS[self.strategy],
            "resources": {k: v.to_dict() for k, v in self.resources.items()},
            "waiting": [p.to_dict() for p in sorted(self.waiting, key=lambda x: (x.urgency, x.arrival_time))[:100]],
            "in_treatment": [p.to_dict() for p in self.in_treatment[:100]],
            "queue_len": len(self.waiting),
            "in_treatment_count": len(self.in_treatment),
            "discharged_total": len(self.discharged),
            "wait_by_urgency": wait_by_urgency,
            "dept_load": dept_load,
            "departments": self.department_analytics(),
            "history": self.history[-100:],
            "events": list(self.events),
            "flags": {"surge": self.surge, "staff_shortage": self.staff_shortage, "auto_failures": self.auto_failures, "icu_constrained": self.icu_constrained},
            "config": {"arrival_rate": self.arrival_rate, "ambulance_rate": self.ambulance_rate, "surge_multiplier": self.surge_multiplier, "shortage_pct": self.shortage_pct, "failure_chance": self.failure_chance},
            "metrics": self.summary_metrics(),
        }

    def summary_metrics(self):
        treated_wait = [p.wait_time for p in self.discharged]
        avg_util = [h["avg_resource_utilization"] for h in self.history]
        max_wait = max([p.wait_time for p in self.discharged] + [self.tick_count - p.arrival_time for p in self.waiting] + [0])
        urgent_waiting = sum(p.urgency <= 2 for p in self.waiting)
        utilization = round(sum(avg_util) / len(avg_util), 1) if avg_util else 0
        # A transparent operational score: fewer urgent waits/conflicts and
        # better utilization increase the score. It is descriptive, not a
        # clinical recommendation.
        pressure_score = round(max(0, 100 - urgent_waiting * 8 - self.conflicts * 0.35 + utilization * 0.18), 1)
        return {
            "strategy": self.strategy,
            "strategy_label": STRATEGY_LABELS[self.strategy],
            "ticks_run": self.tick_count,
            "patients_discharged": len(self.discharged),
            "patients_still_waiting": len(self.waiting),
            "urgent_still_waiting": urgent_waiting,
            "avg_wait_time_treated": round(sum(treated_wait) / len(treated_wait), 2) if treated_wait else 0,
            "max_wait_seen": max_wait,
            "avg_resource_utilization": utilization,
            "capacity_conflicts": self.conflicts,
            "allocations": self.allocations,
            "arrival_throughput": round(len(self.discharged) / max(1, self.tick_count), 2),
            "queue_clearance_rate": round(len(self.discharged) / max(1, self.arrivals_total) * 100, 1),
            "operational_pressure_score": pressure_score,
        }


hospital = Hospital(seed=7)
lock = threading.RLock()


def body():
    return request.get_json(silent=True) or {}


def api_error(exc, status=400):
    return jsonify({"ok": False, "error": str(exc)}), status


@app.errorhandler(Exception)
def handle_error(exc):
    app.logger.exception("MEDFLOW API error")
    return jsonify({"ok": False, "error": str(exc)}), 500


# --------------------------- HTML pages -----------------------

@app.route("/")
@app.route("/dashboard")
def dashboard():
    return render_template_string(APP_HTML, page="dashboard")

@app.route("/patients")
def patients_page():
    return render_template_string(APP_HTML, page="patients")

@app.route("/scenarios")
def scenarios_page():
    return render_template_string(APP_HTML, page="scenarios")

@app.route("/compare")
def compare_page():
    return render_template_string(APP_HTML, page="compare")

@app.route("/how-to")
def how_to_page():
    return render_template_string(APP_HTML, page="how-to")


# --------------------------- REST API --------------------------

@app.get("/api/health")
def api_health():
    return jsonify({"ok": True, "service": "MEDFLOW", "tick": hospital.tick_count, "uptime": round(time.time() - hospital.started_at, 2)})

@app.get("/api/state")
def api_state():
    with lock: return jsonify({"ok": True, "data": hospital.state()})

@app.get("/api/resources")
def api_resources():
    with lock: return jsonify({"ok": True, "data": hospital.state()["resources"]})

@app.get("/api/patients")
def api_patients():
    with lock:
        return jsonify({"ok": True, "data": {"waiting": [p.to_dict() for p in hospital.waiting], "in_treatment": [p.to_dict() for p in hospital.in_treatment], "discharged": [p.to_dict() for p in hospital.discharged[-100:]]}})

@app.get("/api/metrics")
def api_metrics():
    with lock: return jsonify({"ok": True, "data": {"summary": hospital.summary_metrics(), "history": hospital.history[-120:]}})

@app.get("/api/analytics")
def api_analytics():
    with lock: return jsonify({"ok": True, "data": hospital.analytics()})

@app.post("/api/tick")
def api_tick():
    data = body()
    try:
        n = max(1, min(int(data.get("n", 1)), 100))
    except (TypeError, ValueError):
        return api_error(ValueError("n must be an integer between 1 and 100"))
    with lock:
        return jsonify({"ok": True, "data": hospital.tick(n)})

@app.post("/api/reset")
def api_reset():
    global hospital
    data = body()
    with lock:
        try:
            seed = int(data.get("seed", random.randint(1, 999999)))
        except (TypeError, ValueError):
            return api_error(ValueError("seed must be an integer"))
        strategy = data.get("strategy", "urgency_wait")
        if strategy not in STRATEGY_LABELS: strategy = "urgency_wait"
        hospital = Hospital(strategy=strategy, seed=seed)
        return jsonify({"ok": True, "data": hospital.state()})

@app.post("/api/strategy")
def api_strategy():
    with lock:
        try: hospital.set_strategy(body().get("strategy", hospital.strategy))
        except ValueError as e: return api_error(e)
        return jsonify({"ok": True, "data": hospital.state()})

@app.post("/api/config")
def api_config():
    with lock: return jsonify({"ok": True, "data": hospital.configure(body())})

@app.post("/api/demo")
def api_demo():
    """Atomically prepare a deterministic pressure scenario for the Demo button."""
    global hospital
    with lock:
        hospital = Hospital(strategy="urgency_wait", seed=2026)
        hospital.configure({
            "surge": True,
            "staff_shortage": True,
            "auto_failures": True,
            "arrival_rate": 2.0,
            "ambulance_rate": 0.55,
            "failure_chance": 0.07,
        })
        hospital.log("Pressure demo initialized: surge + staff shortage + automatic failures.", "demo")
        # Seed the dashboard with enough data for the first frame. The browser
        # then continues from this exact state without replacing the DOM.
        hospital.tick(4)
        return jsonify({"ok": True, "data": hospital.state()})

@app.post("/api/failure")
def api_failure():
    with lock:
        hospital.trigger_failure(body().get("resource"))
        return jsonify({"ok": True, "data": hospital.state()})

@app.post("/api/resolve_failures")
def api_resolve_failures():
    with lock:
        hospital.resolve_failures()
        return jsonify({"ok": True, "data": hospital.state()})

@app.post("/api/decision")
def api_decision():
    """Return explainable scheduling math for the current queue without mutating state."""
    with lock:
        rows = []
        for p in sorted(hospital.waiting, key=hospital._score)[:25]:
            wait = hospital.tick_count - p.arrival_time
            urgency_component = p.urgency * 10
            wait_component = min(wait * 0.6, 9.5)
            resource_pressure = 0.0
            for name, qty in p.needs.items():
                pool = hospital.resources.get(name)
                if pool and pool.operational:
                    resource_pressure += (1 - pool.available / pool.operational) * 2.5
            if hospital.strategy == "optimization_engine":
                opt = hospital._optimization_components(p)
                raw = opt["benefit"]
                formula = "urgency risk + starvation + scarcity balance − fragmentation − scarce-capacity reserve penalty"
            else:
                raw = urgency_component - wait_component + (resource_pressure if hospital.strategy == "utilization_aware" else 0)
                formula = "urgency × 10 − min(wait × 0.6, 9.5) + resource pressure"
            rows.append({"patient_id": p.id, "urgency": p.urgency_label, "wait_time": wait, "raw_priority": round(raw, 3), "resource_pressure": round(resource_pressure, 3), "can_fit_now": hospital._can_fit(p)})
        return jsonify({"ok": True, "data": {"strategy": hospital.strategy, "formula": formula, "patients": rows}})

@app.post("/api/optimize")
def api_optimize():
    """Bounded beam-search admission plan; never mutates live state."""
    data = body()
    try:
        horizon = max(1, min(int(data.get("horizon", 8)), 30))
        beam_width = max(8, min(int(data.get("beam_width", 48)), 128))
    except (TypeError, ValueError):
        return api_error(ValueError("horizon and beam_width must be numeric"))
    with lock:
        candidates = sorted(hospital.waiting, key=hospital._optimization_score)[:24]
        base = {k: v.available for k, v in hospital.resources.items()}
        beams = [(0.0, base, [])]
        for p in candidates:
            comp = hospital._optimization_components(p)
            next_beams = list(beams)
            for objective, available, selected in beams:
                if not comp["feasible"]:
                    continue
                if all(available.get(r, 0) >= q for r, q in p.needs.items()):
                    nxt = dict(available)
                    for r, q in p.needs.items():
                        nxt[r] -= q
                    # Small plan-level bonus rewards using several compatible
                    # resources without consuming the final ICU/OR reserve.
                    balance = sum(max(0, nxt[r]) for r in p.needs) * 0.02
                    next_beams.append((objective + comp["benefit"] + balance, nxt, selected + [p.id]))
            next_beams.sort(key=lambda x: (-x[0], len(x[2])))
            beams = next_beams[:beam_width]
        best = max(beams, key=lambda x: x[0]) if beams else (0, base, [])
        selected_ids = set(best[2])
        projected = {k: base[k] - best[1][k] for k in base}
        rows = []
        for p in candidates:
            comp = hospital._optimization_components(p)
            rows.append({"patient_id": p.id, "name": p.name, "department": p.department,
                         "urgency": p.urgency_label, "wait_time": hospital.tick_count-p.arrival_time,
                         "selected": p.id in selected_ids, **comp})
        return jsonify({"ok": True, "data": {
            "horizon": horizon, "beam_width": beam_width,
            "objective": "urgency protection + waiting-time fairness + resource balance − scarcity/fragmentation/reserve penalties",
            "objective_value": round(best[0], 3),
            "recommended_patient_ids": best[2],
            "selected_count": len(best[2]),
            "candidates": rows,
            "projected_allocations": projected,
            "remaining_capacity": best[1],
        }})

@app.get("/api/departments")
def api_departments():
    with lock:
        return jsonify({"ok": True, "data": hospital.department_analytics()})

@app.get("/api/forecast")
def api_forecast():
    try:
        horizon = max(1, min(int(request.args.get("horizon", 6)), 48))
    except (TypeError, ValueError):
        return api_error(ValueError("horizon must be an integer"))
    with lock:
        return jsonify({"ok": True, "data": hospital.forecast(horizon)})

@app.get("/api/events")
def api_events():
    try:
        limit = max(1, min(int(request.args.get("limit", 30)), 100))
    except (TypeError, ValueError):
        return api_error(ValueError("limit must be an integer"))
    with lock:
        return jsonify({"ok": True, "data": list(hospital.events)[:limit]})

@app.post("/api/scenario/run")
def api_scenario_run():
    """Run a complete what-if scenario and return a compact operational report."""
    data = body()
    try:
        ticks = max(1, min(int(data.get("ticks", 60)), 1000))
        seed = int(data.get("seed", 42))
    except (TypeError, ValueError):
        return api_error(ValueError("ticks and seed must be numeric"))
    strategy = data.get("strategy", "optimization_engine")
    if strategy not in STRATEGY_LABELS:
        return api_error(ValueError("Unknown scheduling strategy"))
    h = Hospital(strategy=strategy, seed=seed)
    scenario_keys = ("surge", "staff_shortage", "auto_failures", "icu_constrained",
                     "arrival_rate", "ambulance_rate", "surge_multiplier", "shortage_pct", "failure_chance")
    h.configure({k: data[k] for k in scenario_keys if k in data})
    h.tick(ticks)
    return jsonify({"ok": True, "data": {
        "summary": h.summary_metrics(),
        "analytics": h.analytics(),
        "departments": h.department_analytics(),
        "forecast": h.forecast(6),
        "history": h.history,
    }})

@app.post("/api/import.csv")
def api_import_csv():
    if "file" not in request.files:
        return api_error(ValueError("Attach a CSV file using the form field 'file'. Example columns: name,urgency,department,duration,bed,icu,or,doctor,nurse,ambulance"))
    uploaded=request.files["file"]
    if not uploaded.filename or not uploaded.filename.lower().endswith(".csv"):
        return api_error(ValueError("Please upload a .csv file."))
    try:
        raw=uploaded.stream.read(2_000_000).decode("utf-8-sig")
        reader=csv.DictReader(io.StringIO(raw))
        if not reader.fieldnames:
            raise ValueError("CSV has no header row.")
        required={"name","urgency","department"}
        missing=required-set(h.strip().lower() for h in reader.fieldnames if h)
        if missing: raise ValueError("Missing required columns: "+", ".join(sorted(missing)))
        rows=[]
        for row in reader:
            rows.append({(k or "").strip().lower(): (v or "").strip() for k,v in row.items()})
        with lock:
            result=hospital.import_patients(rows)
            return jsonify({"ok":True,"data":{**result,"state":hospital.state()}})
    except UnicodeDecodeError:
        return api_error(ValueError("CSV must be UTF-8 encoded."))
    except Exception as exc:
        return api_error(exc)

@app.post("/api/simulate")
def api_simulate():
    """Run an isolated scenario without changing the live dashboard state."""
    data = body()
    ticks = max(1, min(int(data.get("ticks", 60)), 1000))
    seed = int(data.get("seed", 42))
    strategy = data.get("strategy", "urgency_wait")
    if strategy not in STRATEGY_LABELS:
        return api_error(ValueError("Unknown scheduling strategy"))
    h = Hospital(strategy=strategy, seed=seed)
    h.configure({k: data[k] for k in ("surge", "staff_shortage", "arrival_rate", "ambulance_rate", "surge_multiplier", "shortage_pct", "failure_chance", "auto_failures", "icu_constrained") if k in data})
    h.tick(ticks)
    return jsonify({"ok": True, "data": {"summary": h.summary_metrics(), "history": h.history}})

@app.post("/api/compare")
def api_compare():
    data = body()
    ticks = max(5, min(int(data.get("ticks", 120)), 1000))
    seed = int(data.get("seed", 42))
    strategies = [s for s in data.get("strategies", list(STRATEGY_LABELS)) if s in STRATEGY_LABELS]
    if not strategies:
        return api_error(ValueError("At least one valid scheduling strategy is required"))
    scenario = {k: data[k] for k in ("surge", "staff_shortage", "arrival_rate", "ambulance_rate", "surge_multiplier", "shortage_pct", "failure_chance", "auto_failures", "icu_constrained") if k in data}
    results = []
    histories = {}
    for strategy in strategies:
        h = Hospital(strategy=strategy, seed=seed)
        h.configure(scenario)
        h.tick(ticks)
        results.append(h.summary_metrics())
        histories[strategy] = h.history
    return jsonify({"ok": True, "data": {"ticks": ticks, "seed": seed, "results": results, "histories": histories}})

@app.get("/api/patient-template.csv")
def patient_template():
    output=io.StringIO(); writer=csv.writer(output)
    writer.writerow(["name","urgency","department","arrival_time","duration","bed","icu","or","doctor","nurse","ambulance"])
    writer.writerow(["Example Critical",1,"Emergency",0,8,1,1,0,1,2,1])
    writer.writerow(["Example Routine",4,"General",0,2,1,0,0,0,1,0])
    return Response(output.getvalue(),mimetype="text/csv",headers={"Content-Disposition":"attachment; filename=medflow_patient_template.csv"})

@app.get("/api/export.csv")
def export_csv():
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["tick", "queue", "urgent_waiting", "in_treatment", "discharged", "avg_wait", "avg_utilization", "capacity_conflicts"])
    for h in hospital.history:
        writer.writerow([h["tick"], h["queue_len"], h["urgent_waiting"], h["in_treatment"], h["discharged_total"], h["avg_wait"], h["avg_resource_utilization"], h["capacity_conflicts"]])
    return Response(output.getvalue(), mimetype="text/csv", headers={"Content-Disposition": "attachment; filename=medflow_metrics.csv"})

# Backward-compatible aliases for older frontend integrations.
@app.post("/api/v1/tick")
def v1_tick(): return api_tick()
@app.get("/api/v1/state")
def v1_state(): return api_state()
@app.get("/api/v1/resources")
def v1_resources(): return api_resources()
@app.get("/api/v1/patients")
def v1_patients(): return api_patients()
@app.get("/api/v1/metrics")
def v1_metrics(): return api_metrics()


APP_HTML = r'''<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover">
<title>MEDFLOW — Hospital Resource Management Simulator</title>
<meta name="description" content="Real-time hospital patient prioritization and resource allocation simulator.">
<style>
:root{--bg:#070a0f;--surface:#0e131b;--surface2:#151b25;--surface3:#1b2330;--line:#283140;--text:#f4f7fb;--muted:#9ba7b8;--accent:#64e6d5;--blue:#8da2ff;--red:#ff6d7a;--amber:#f4bd62;--green:#64e6a2;--shadow:0 24px 70px rgba(0,0,0,.30);--ease:cubic-bezier(.22,1,.36,1)}
:root.light{--bg:#f5f7fa;--surface:#fff;--surface2:#f1f4f8;--surface3:#e9edf3;--line:#dbe1e9;--text:#151a23;--muted:#637084;--accent:#078f83;--blue:#566ed5;--red:#d63e4d;--amber:#ae7316;--green:#1a9b61;--shadow:0 20px 60px rgba(30,45,65,.10)}
*{box-sizing:border-box}html{scroll-behavior:smooth}body{margin:0;background:var(--bg);color:var(--text);font:14px/1.5 Inter,ui-sans-serif,system-ui,-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;transition:background .45s ease,color .45s ease;overflow-x:hidden}button,input,select{font:inherit}button{cursor:pointer}.app{min-height:100vh}.top{position:fixed;z-index:100;top:0;left:0;right:0;height:72px;padding:0 26px;display:flex;align-items:center;gap:20px;background:color-mix(in srgb,var(--bg) 78%,transparent);backdrop-filter:blur(22px) saturate(170%);border-bottom:1px solid color-mix(in srgb,var(--line) 70%,transparent)}.brand{display:flex;align-items:center;gap:11px;min-width:205px;cursor:pointer}.mark{width:34px;height:34px;border-radius:11px;display:grid;place-items:center;background:linear-gradient(135deg,var(--accent),var(--blue));color:#061412;font-weight:900;font-size:11px;box-shadow:0 7px 20px color-mix(in srgb,var(--accent) 20%,transparent)}.brand b{font-size:15px;letter-spacing:.02em}.brand small{display:block;color:var(--muted);font-size:10px}.nav{display:flex;gap:4px;align-items:center;flex:1}.nav a{position:relative;padding:9px 13px;border-radius:10px;color:var(--muted);text-decoration:none;transition:all .28s var(--ease)}.nav a:hover,.nav a.active{color:var(--text);background:var(--surface2)}.nav a.active:after{content:"";position:absolute;left:15px;right:15px;bottom:4px;height:2px;border-radius:2px;background:var(--accent)}.top-actions{display:flex;align-items:center;gap:8px}.icon{width:38px;height:38px;border:1px solid var(--line);border-radius:50%;background:var(--surface);color:var(--muted);display:grid;place-items:center;transition:all .25s var(--ease)}.icon:hover{color:var(--accent);border-color:var(--accent);transform:translateY(-1px)}.main{padding:108px 28px 70px;max-width:1500px;margin:auto}.view{animation:pageIn .55s var(--ease)}@keyframes pageIn{from{opacity:0;transform:translateY(12px) scale(.995)}to{opacity:1;transform:none}}.hero{min-height:calc(100vh - 150px);display:grid;align-items:center;grid-template-columns:1.15fr .85fr;gap:40px}.eyebrow{color:var(--accent);font-size:11px;font-weight:800;letter-spacing:.15em;text-transform:uppercase}.hero h1{font-size:clamp(42px,6.5vw,82px);line-height:.98;letter-spacing:-.055em;margin:14px 0 20px}.hero h1 span{background:linear-gradient(120deg,var(--accent),var(--blue));-webkit-background-clip:text;background-clip:text;color:transparent}.hero p{max-width:680px;color:var(--muted);font-size:16px;line-height:1.7}.hero-actions{display:flex;gap:10px;flex-wrap:wrap;margin-top:28px}.btn{border:1px solid var(--line);background:var(--surface2);color:var(--text);padding:10px 15px;border-radius:11px;transition:transform .2s var(--ease),background .25s,border-color .25s,box-shadow .25s}.btn:hover{transform:translateY(-2px);border-color:var(--accent)}.btn:active{transform:scale(.97)}.btn.primary{background:var(--accent);color:#061412;border-color:var(--accent);font-weight:800;box-shadow:0 10px 30px color-mix(in srgb,var(--accent) 18%,transparent)}.btn.danger{color:var(--red);border-color:color-mix(in srgb,var(--red) 50%,var(--line))}.hero-card{padding:22px;border:1px solid var(--line);border-radius:26px;background:linear-gradient(145deg,var(--surface),var(--surface2));box-shadow:var(--shadow);transform:perspective(900px) rotateY(-3deg);transition:transform .7s var(--ease)}.hero-card:hover{transform:perspective(900px) rotateY(0) translateY(-4px)}.orb{height:240px;border-radius:20px;background:radial-gradient(circle at 50% 45%,color-mix(in srgb,var(--accent) 24%,transparent),transparent 28%),radial-gradient(circle at 30% 70%,color-mix(in srgb,var(--blue) 22%,transparent),transparent 34%),var(--surface2);display:grid;place-items:center;position:relative;overflow:hidden}.orb:before,.orb:after{content:"";position:absolute;border:1px solid color-mix(in srgb,var(--accent) 30%,transparent);border-radius:50%;animation:pulse 4s ease-in-out infinite}.orb:before{width:170px;height:170px}.orb:after{width:250px;height:250px;animation-delay:1.2s}@keyframes pulse{0%,100%{transform:scale(.88);opacity:.3}50%{transform:scale(1.05);opacity:.8}}.orb strong{font-size:52px;letter-spacing:-.05em;z-index:1}.stats{display:grid;grid-template-columns:repeat(4,1fr);gap:12px;margin-top:18px}.stat,.card{background:var(--surface);border:1px solid var(--line);border-radius:18px;box-shadow:0 8px 28px rgba(0,0,0,.08);transition:transform .35s var(--ease),border-color .3s,background .4s}.stat{padding:17px}.stat:hover,.card:hover{transform:translateY(-3px);border-color:color-mix(in srgb,var(--accent) 40%,var(--line))}.stat small,.label{color:var(--muted);font-size:11px}.stat strong{display:block;font:600 25px ui-monospace,SFMono-Regular,Menlo,monospace;margin-top:4px}.section-title{display:flex;justify-content:space-between;align-items:end;gap:15px;margin:20px 0 16px}.section-title h2{font-size:27px;letter-spacing:-.03em;margin:0}.section-title p{color:var(--muted);margin:4px 0 0}.grid{display:grid;gap:14px}.g2{grid-template-columns:repeat(2,minmax(0,1fr))}.g3{grid-template-columns:repeat(3,minmax(0,1fr))}.g4{grid-template-columns:repeat(4,minmax(0,1fr))}.card{padding:20px}.card h3{font-size:14px;margin:0 0 14px}.resource{display:grid;grid-template-columns:90px 1fr 60px;align-items:center;gap:12px;margin:15px 0}.bar{height:8px;background:var(--surface3);border-radius:99px;overflow:hidden}.fill{height:100%;border-radius:inherit;background:linear-gradient(90deg,var(--accent),var(--blue));transition:width .7s var(--ease)}.mono{font-family:ui-monospace,SFMono-Regular,Menlo,monospace}.table-wrap{overflow:auto;max-height:520px}table{width:100%;border-collapse:collapse;min-width:720px}th,td{text-align:left;padding:11px 10px;border-bottom:1px solid var(--line);font-size:12px}th{color:var(--muted);font-size:10px;text-transform:uppercase;letter-spacing:.08em;position:sticky;top:0;background:var(--surface)}td{color:var(--muted)}td b{color:var(--text)}.chip{display:inline-flex;padding:4px 8px;border-radius:99px;font-size:10px;font-weight:800}.critical{background:color-mix(in srgb,var(--red) 18%,transparent);color:var(--red)}.urgent{background:color-mix(in srgb,var(--amber) 18%,transparent);color:var(--amber)}.moderate{background:color-mix(in srgb,var(--blue) 18%,transparent);color:var(--blue)}.low{background:color-mix(in srgb,var(--green) 18%,transparent);color:var(--green)}.events{display:flex;flex-direction:column;gap:6px;max-height:410px;overflow:auto}.event{padding:9px 10px;border-left:2px solid var(--line);background:var(--surface2);border-radius:0 9px 9px 0;animation:eventIn .35s var(--ease)}@keyframes eventIn{from{opacity:0;transform:translateX(-8px)}to{opacity:1;transform:none}}.event small{color:var(--muted);margin-right:7px}.event.failure{border-color:var(--red)}.event.surge{border-color:var(--blue)}.event.shortage{border-color:var(--amber)}.switch-row{display:flex;align-items:center;justify-content:space-between;padding:11px 0;border-bottom:1px solid var(--line)}.switch{position:relative;width:42px;height:24px}.switch input{opacity:0;width:0;height:0}.switch span{position:absolute;inset:0;border:1px solid var(--line);background:var(--surface3);border-radius:99px;transition:.25s}.switch span:before{content:"";position:absolute;width:16px;height:16px;left:3px;top:3px;border-radius:50%;background:var(--muted);transition:.25s}.switch input:checked+span{background:color-mix(in srgb,var(--accent) 25%,transparent);border-color:var(--accent)}.switch input:checked+span:before{transform:translateX(18px);background:var(--accent)}input[type=range]{width:100%;accent-color:var(--accent)}select{width:100%;background:var(--surface2);color:var(--text);border:1px solid var(--line);padding:10px;border-radius:10px}.strategy{display:grid;grid-template-columns:repeat(3,1fr);gap:8px}.strategy button{padding:12px;border:1px solid var(--line);border-radius:12px;background:var(--surface2);color:var(--muted);text-align:left;transition:.25s var(--ease)}.strategy button.active{border-color:var(--accent);background:color-mix(in srgb,var(--accent) 12%,var(--surface2));color:var(--text);box-shadow:inset 0 0 0 1px color-mix(in srgb,var(--accent) 30%,transparent)}.chart{height:240px;display:flex;align-items:end;gap:3px;padding-top:20px;overflow:hidden}.bar-col{flex:1;min-width:3px;background:linear-gradient(to top,var(--accent),var(--blue));border-radius:5px 5px 0 0;transition:height .65s var(--ease);opacity:.85}.decision-panel{display:grid;gap:8px}.decision-row{display:grid;grid-template-columns:1fr 90px 90px 90px;gap:10px;padding:10px;border:1px solid var(--line);border-radius:12px;background:var(--surface2);font-size:11px}.decision-row b{font-family:ui-monospace,SFMono-Regular,Menlo,monospace}.compare-card{padding:16px;border:1px solid var(--line);border-radius:15px;background:var(--surface2);animation:pageIn .45s var(--ease)}.compare-card h4{margin:0 0 10px}.metric-row{display:flex;justify-content:space-between;padding:6px 0;color:var(--muted);border-bottom:1px solid var(--line)}.metric-row b{color:var(--text)}.toast{position:fixed;z-index:150;left:50%;top:88px;transform:translate(-50%,-145%) scale(.96);padding:12px 17px;border:1px solid var(--line);background:color-mix(in srgb,var(--surface) 92%,transparent);backdrop-filter:blur(18px) saturate(150%);box-shadow:var(--shadow);border-radius:15px;color:var(--muted);transition:transform .42s var(--ease),opacity .32s ease;opacity:0;pointer-events:none;max-width:min(92vw,600px);text-align:center;line-height:1.4}.toast.show{transform:translate(-50%,0) scale(1);opacity:1}.toast.hide{transform:translate(-50%,-145%) scale(.96);opacity:0}.toast[data-kind="error"]{border-color:color-mix(in srgb,var(--red) 55%,var(--line));color:var(--red)}.toast[data-kind="success"]{border-color:color-mix(in srgb,var(--green) 55%,var(--line));color:var(--green)}.loading{cursor:progress}.loading > .view{pointer-events:none}.loading::after{content:"";position:fixed;z-index:140;left:50%;top:72px;width:110px;height:2px;transform:translateX(-50%);background:linear-gradient(90deg,transparent,var(--accent),transparent);animation:loadSweep 1s ease-in-out infinite}.empty{padding:40px;text-align:center;color:var(--muted)}.footer{color:var(--muted);text-align:center;padding:30px 0;font-size:11px}.back{display:inline-flex;gap:7px;align-items:center;color:var(--muted);text-decoration:none;margin-bottom:12px}.mobile-menu{display:none}
.page-title{font-size:clamp(38px,6vw,64px);line-height:1;letter-spacing:-.055em;margin:10px 0}.lead{max-width:720px;color:var(--muted);font-size:16px;line-height:1.7}.howto-head{display:flex;justify-content:space-between;align-items:end;gap:24px;margin:18px 0 28px}.status-pill{padding:8px 12px;border:1px solid var(--line);border-radius:999px;background:var(--surface2);color:var(--muted);font-size:11px;white-space:nowrap}.status-pill.online{color:var(--green);border-color:color-mix(in srgb,var(--green) 45%,var(--line));background:color-mix(in srgb,var(--green) 9%,var(--surface))}.status-pill.offline{color:var(--red);border-color:color-mix(in srgb,var(--red) 45%,var(--line));background:color-mix(in srgb,var(--red) 9%,var(--surface))}.guide-grid{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:14px}.guide-step{position:relative;min-height:190px;padding:24px;border:1px solid var(--line);border-radius:22px;background:linear-gradient(145deg,var(--surface),var(--surface2));box-shadow:var(--shadow);transition:transform .4s var(--ease),border-color .3s}.guide-step:hover{transform:translateY(-5px);border-color:color-mix(in srgb,var(--accent) 45%,var(--line))}.guide-step>span{font:700 11px ui-monospace,monospace;color:var(--accent);letter-spacing:.12em}.guide-step h3{margin:22px 0 8px;font-size:18px}.guide-step p{color:var(--muted);line-height:1.65;margin:0}.guide-note{margin-top:14px;display:flex;justify-content:space-between;align-items:center;gap:18px}.guide-note p{color:var(--muted);max-width:820px;margin:5px 0 0}@media(max-width:980px){.guide-grid{grid-template-columns:1fr 1fr}.howto-head{align-items:flex-start;flex-direction:column}}@media(max-width:680px){.guide-grid{grid-template-columns:1fr}.guide-note{align-items:flex-start;flex-direction:column}}

.tour-spotlight{position:fixed;z-index:301;pointer-events:none;border:2px solid var(--accent);border-radius:16px;box-shadow:0 0 0 9999px rgba(3,7,12,.48),0 0 0 6px color-mix(in srgb,var(--accent) 18%,transparent);transition:all .35s var(--ease);display:none}.tour-spotlight.show{display:block}.tour{position:fixed;inset:0;z-index:300;display:none;align-items:center;justify-content:center;padding:20px}.tour.show{display:flex}.tour-backdrop{position:absolute;inset:0;background:rgba(3,7,12,.62);backdrop-filter:blur(12px)}.tour-card{position:relative;width:min(560px,94vw);padding:30px;border:1px solid var(--line);border-radius:28px;background:linear-gradient(145deg,var(--surface),var(--surface2));box-shadow:0 35px 100px rgba(0,0,0,.4);animation:tourIn .5s var(--ease)}.tour-target{position:relative!important;z-index:360!important;outline:3px solid var(--accent)!important;outline-offset:7px!important;box-shadow:0 0 0 9999px rgba(4,8,15,.24),0 0 0 10px color-mix(in srgb,var(--accent) 22%,transparent),0 18px 55px rgba(0,0,0,.28)!important;border-radius:14px!important;transition:outline .2s ease,box-shadow .2s ease!important}.tour-card{z-index:370}@keyframes tourIn{from{opacity:0;transform:translateY(18px) scale(.97)}to{opacity:1;transform:none}}.tour-stepno{margin-top:12px;color:var(--muted);font:700 11px ui-monospace,monospace}.tour-card h2{font-size:32px;letter-spacing:-.04em;margin:8px 0}.tour-card p{color:var(--muted);line-height:1.7}.tour-actions{display:flex;justify-content:space-between;align-items:center;margin-top:24px}.tour-nav{border:1px solid var(--line);background:var(--surface2);color:var(--text);padding:8px 12px;border-radius:10px}.strategy button.active{transform:translateY(-1px);box-shadow:0 10px 28px color-mix(in srgb,var(--accent) 12%,transparent),inset 0 0 0 1px var(--accent);position:relative}.strategy button.active:before{content:'✓';position:absolute;right:10px;top:8px;color:var(--accent);font-weight:900}.import-zone{border:1px dashed color-mix(in srgb,var(--accent) 55%,var(--line));border-radius:18px;padding:18px;background:color-mix(in srgb,var(--accent) 4%,var(--surface2));display:flex;gap:12px;align-items:center;justify-content:space-between}.import-preview{margin-top:12px;max-height:220px;overflow:auto}.opt-card{padding:16px;border:1px solid var(--line);border-radius:15px;background:var(--surface2)}.opt-card strong{font-size:22px}.opt-list{display:grid;gap:7px;margin-top:12px}.opt-row{display:grid;grid-template-columns:1fr 100px 100px 100px;gap:8px;padding:9px;border-bottom:1px solid var(--line);font-size:11px}
@media(max-width:980px){.hero{grid-template-columns:1fr}.hero-card{display:none}.g4{grid-template-columns:repeat(2,1fr)}.nav a{padding:8px 8px}.brand{min-width:auto}.brand small{display:none}.strategy{grid-template-columns:1fr}.g3,.g2{grid-template-columns:1fr}}
@media(max-width:680px){.top{height:62px;padding:0 13px}.nav{display:none}.mobile-menu{display:grid;margin-left:auto}.top-actions{margin-left:0}.main{padding:85px 14px 50px}.hero{min-height:auto;padding:35px 0}.hero h1{font-size:46px}.stats{grid-template-columns:repeat(2,1fr)}.g4{grid-template-columns:1fr 1fr}.section-title{align-items:flex-start;flex-direction:column}}
@media(prefers-reduced-motion:reduce){*,*:before,*:after{animation-duration:.001ms!important;transition-duration:.001ms!important;scroll-behavior:auto!important}}

/* Apple-style scroll choreography: elements reveal once as they enter the viewport. */
.reveal{opacity:0;transform:translate3d(0,34px,0) scale(.985);filter:blur(5px);transition:opacity .72s var(--ease),transform .9s var(--ease),filter .9s var(--ease);transition-delay:var(--delay,0ms);will-change:transform,opacity,filter}.reveal.is-visible{opacity:1;transform:none;filter:none}.reveal-left{transform:translate3d(-42px,0,0) scale(.985)}.reveal-right{transform:translate3d(42px,0,0) scale(.985)}.reveal-scale{transform:scale(.93) translateY(18px)}.reveal.is-visible.reveal-left,.reveal.is-visible.reveal-right,.reveal.is-visible.reveal-scale{transform:none}.reveal.from-bottom{transform:translate3d(0,34px,0) scale(.985)}.view{min-height:calc(100vh - 160px);transform-origin:50% 0}.view.page-switch{animation:pageSwitch .65s var(--ease)}@keyframes pageSwitch{from{opacity:0;transform:translateY(18px) scale(.992)}to{opacity:1;transform:none}}::view-transition-old(root){animation:pageOut .34s var(--ease) both}::view-transition-new(root){animation:pageIn .58s var(--ease) both}@keyframes pageOut{to{opacity:0;transform:translateY(-8px) scale(.995)}}@keyframes loadSweep{0%{opacity:0;transform:translateX(-50%) scaleX(.2)}50%{opacity:1;transform:translateX(-50%) scaleX(1)}100%{opacity:0;transform:translateX(-50%) scaleX(.2)}}button:disabled{opacity:.55;cursor:wait;transform:none!important}@media (prefers-reduced-motion:reduce){.reveal,.reveal.is-visible,.view,.hero-card,.stat,.card,.btn{transition:none!important;animation:none!important;transform:none!important;filter:none!important}html{scroll-behavior:auto}}

.viz-grid{display:grid;grid-template-columns:1.2fr 1fr 1fr;gap:12px}.viz-card{border:1px solid var(--line);background:var(--surface2);border-radius:18px;padding:14px;min-height:220px}.viz-card h4{margin:0 0 8px}.viz-card canvas{width:100%;height:180px}.flow-pictorial{display:flex;align-items:center;justify-content:center;gap:7px;min-height:180px;flex-wrap:wrap}.flow-node{min-width:78px;text-align:center;padding:12px 8px;border-radius:16px;border:1px solid var(--line);background:var(--surface)}.flow-icon{font-size:25px;display:block;margin-bottom:4px}.flow-arrow{font-size:22px;color:var(--muted)}.analytics-grid{display:grid;grid-template-columns:1fr 1fr;gap:12px}.mini-panel{border:1px solid var(--line);border-radius:16px;padding:14px;background:var(--surface2)}.risk-meter{height:8px;background:var(--surface);border-radius:99px;overflow:hidden;margin-top:8px}.risk-meter i{display:block;height:100%;background:var(--accent);border-radius:99px}.action-item{padding:8px 0;border-bottom:1px solid var(--line);font-size:13px}.action-item:last-child{border-bottom:0}@media(max-width:980px){.viz-grid{grid-template-columns:1fr 1fr}.viz-card:last-child{grid-column:1/-1}}@media(max-width:680px){.viz-grid,.analytics-grid{grid-template-columns:1fr}.viz-card:last-child{grid-column:auto}}
</style>
</head>
<body>
<div id="toast" class="toast"></div>
<div id="tourSpotlight" class="tour-spotlight"></div><div id="tour" class="tour" aria-hidden="true"><div class="tour-backdrop"></div><div class="tour-card"><div class="eyebrow">MEDFLOW TOUR</div><div id="tourStepNo" class="tour-stepno"></div><h2 id="tourTitle"></h2><p id="tourText"></p><div class="tour-actions"><button class="btn" id="tourSkip">Skip</button><div><button class="btn" id="tourPrev">Back</button><button class="btn primary" id="tourNext">Next</button></div></div></div></div>
<header class="top">
  <div class="brand" onclick="navigate('/dashboard')"><div class="mark">MF</div><div><b>MEDFLOW</b><small>Hospital Resource Simulator</small></div></div>
  <nav class="nav" id="nav"><a href="/dashboard" data-route="/dashboard">Dashboard</a><a href="/patients" data-route="/patients">Patients</a><a href="/scenarios" data-route="/scenarios">Scenarios</a><a href="/compare" data-route="/compare">Compare</a><a href="/how-to" data-route="/how-to">How to Use</a></nav>
  <div class="top-actions"><button class="icon" id="theme" title="Toggle light/dark">☼</button><button class="icon mobile-menu" id="menu">☰</button></div>
</header>
<main class="main"><div id="app"></div><div class="footer">MEDFLOW • Prioritize Patients • Optimize Resources</div></main>
<script>
const URGENCY_LEVELS = {1: "Critical", 2: "Urgent", 3: "Moderate", 4: "Low"};
const initialPage = {{ page|tojson }};
const routes = {dashboard:'/dashboard',patients:'/patients',scenarios:'/scenarios',compare:'/compare', 'how-to':'/how-to'};
let state=null, autoTimer=null, busy=false, aborter=null, autoRunToken=0, revealObserver=null;
const $=s=>document.querySelector(s), esc=s=>String(s??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
function toast(msg,kind='info'){const t=$('#toast');if(!t)return;clearTimeout(toast.t);clearTimeout(toast.hide);t.classList.remove('hide');t.textContent=msg;t.dataset.kind=kind;requestAnimationFrame(()=>t.classList.add('show'));const duration=kind==='error'?4200:3000;toast.t=setTimeout(()=>{t.classList.remove('show');t.classList.add('hide');toast.hide=setTimeout(()=>{t.textContent='';t.classList.remove('hide');},450)},duration)}
function setLoading(v){busy=v;document.body.classList.toggle('is-busy',v);$('#app')?.classList.toggle('loading',v)}
async function api(url,opts={}){const headers={...(opts.headers||{})};if(opts.body && !(opts.body instanceof FormData))headers['Content-Type']='application/json';let r;try{r=await fetch(url,{...opts,cache:'no-store',headers})}catch(e){throw Error('Connection unavailable. Please try again.')}let j;try{j=await r.json()}catch{throw Error('The server returned an invalid response. Please try again.')}if(!r.ok||j.ok===false)throw Error(j.error||`Request failed (${r.status})`);return j.data}
async function refresh(){try{state=await api('/api/state');render()}catch(e){toast('Unable to update simulation. Please try again.','error')}}
function navActive(path){document.querySelectorAll('[data-route]').forEach(a=>a.classList.toggle('active',a.dataset.route===path))}
function navigate(path,push=true){if(push)history.pushState({},'',path);navActive(path);renderPage(path.slice(1)||'dashboard')}
window.addEventListener('popstate',()=>{navActive(location.pathname);renderPage(location.pathname.slice(1)||'dashboard')});

document.querySelectorAll('[data-route]').forEach(a=>a.addEventListener('click',e=>{e.preventDefault();navigate(a.dataset.route)}));
$('#theme').onclick=()=>{document.documentElement.classList.toggle('light');localStorage.setItem('medflow-theme',document.documentElement.classList.contains('light')?'light':'dark')};
if(localStorage.getItem('medflow-theme')==='light')document.documentElement.classList.add('light');
$('#menu').onclick=()=>{$('.nav').style.display=$('.nav').style.display==='flex'?'none':'flex';$('.nav').style.position='absolute';$('.nav').style.top='62px';$('.nav').style.left='10px';$('.nav').style.right='10px';$('.nav').style.padding='10px';$('.nav').style.border='1px solid var(--line)';$('.nav').style.background='var(--surface)';$('.nav').style.borderRadius='14px'};

function controls(){return `<div class="card"><h3>Simulation controls</h3><div class="strategy" id="strategy">${Object.entries({urgency_only:'Urgency Only',urgency_wait:'Urgency + Wait Time',utilization_aware:'Urgency + Wait + Utilization',optimization_engine:'Optimization Engine'}).map(([k,v])=>`<button data-strategy="${k}" class="${state.strategy===k?'active':''}">${v}</button>`).join('')}</div><div style="display:flex;gap:8px;margin-top:12px;flex-wrap:wrap"><button class="btn primary" id="tick1">Advance 1</button><button class="btn" id="tick10">Advance 10</button><button class="btn" id="auto">${autoTimer?'Stop auto-run':'Start auto-run'}</button><button class="btn danger" id="reset">Reset</button><button class="btn" id="tourInline">Guided Tour</button><a class="btn" href="/api/export.csv">Export CSV</a></div></div>`}
function render(){renderPage(location.pathname.slice(1)||initialPage||'dashboard',true)}
function installReveals(){
  if(revealObserver){revealObserver.disconnect();revealObserver=null}
  const els=document.querySelectorAll('.reveal');
  if(!els.length)return;
  if(!('IntersectionObserver' in window)){els.forEach(e=>e.classList.add('is-visible'));return}
  revealObserver=new IntersectionObserver((entries)=>{entries.forEach((entry)=>{
    if(entry.isIntersecting){entry.target.classList.add('is-visible')}
    else if(entry.boundingClientRect.top > 0 || entry.boundingClientRect.bottom < 0){entry.target.classList.remove('is-visible')}
  })},{root:null,rootMargin:'-8% 0px -10% 0px',threshold:.06});
  els.forEach((el,i)=>{el.style.setProperty('--delay',`${Math.min(i%6,5)*65}ms`);revealObserver.observe(el)})
}
function renderPage(page,animate=true){
  navActive('/'+page);
  const swap=()=>{
    const view=document.createElement('div');
    view.className='view'+(animate?' page-switch':'');
    if(page==='dashboard')view.innerHTML=dashboardHTML();
    else if(page==='patients')view.innerHTML=patientsHTML();
    else if(page==='scenarios')view.innerHTML=scenariosHTML();
    else if(page==='compare')view.innerHTML=compareHTML();
    else if(page==='how-to')view.innerHTML=howToHTML();
    else {history.replaceState({},'', '/dashboard');view.innerHTML=dashboardHTML();page='dashboard'}
    $('#app').replaceChildren(view);
    wireCommon();
    if(page==='dashboard')wireDashboard();
    if(page==='patients')wirePatients();
    if(page==='scenarios')wireScenarios();
    if(page==='compare')wireCompare();
    if(page==='how-to')wireHowTo();
    requestAnimationFrame(()=>{installReveals(); window.scrollTo({top:0,behavior:'instant'})});
  };
  if(animate && document.startViewTransition){document.startViewTransition(swap)}else swap();
}

function drawLineCanvas(canvas, seriesA, seriesB){if(!canvas)return;const ctx=canvas.getContext('2d'),dpr=window.devicePixelRatio||1,w=canvas.clientWidth||500,h=180;canvas.width=w*dpr;canvas.height=h*dpr;ctx.scale(dpr,dpr);ctx.clearRect(0,0,w,h);const pad={l:30,r:10,t:12,b:24},cw=w-pad.l-pad.r,ch=h-pad.t-pad.b;ctx.strokeStyle=getComputedStyle(document.documentElement).getPropertyValue('--line');ctx.lineWidth=1;for(let i=0;i<4;i++){const y=pad.t+ch*i/3;ctx.beginPath();ctx.moveTo(pad.l,y);ctx.lineTo(w-pad.r,y);ctx.stroke()}const max=Math.max(1,...seriesA,...seriesB);const draw=(arr,offset)=>{ctx.beginPath();arr.forEach((v,i)=>{const x=pad.l+(arr.length===1?0:i/(arr.length-1))*cw,y=pad.t+ch-(v/max)*ch;i?ctx.lineTo(x,y):ctx.moveTo(x,y)});ctx.strokeStyle=offset?'#7c8cff':'#55d6be';ctx.lineWidth=2.2;ctx.stroke()};draw(seriesA,0);draw(seriesB,1);ctx.fillStyle=getComputedStyle(document.documentElement).getPropertyValue('--muted');ctx.font='10px ui-monospace';ctx.fillText('queue',pad.l, h-6);ctx.fillText('util.',pad.l+55,h-6)}
function drawResourceCanvas(canvas){if(!canvas||!state)return;const ctx=canvas.getContext('2d'),dpr=window.devicePixelRatio||1,w=canvas.clientWidth||500,h=180;canvas.width=w*dpr;canvas.height=h*dpr;ctx.scale(dpr,dpr);ctx.clearRect(0,0,w,h);const rows=Object.values(state.resources),pad=16,bw=(w-pad*2)/Math.max(1,rows.length)-10;rows.forEach((r,i)=>{const x=pad+i*((w-pad*2)/rows.length)+5,base=h-28,maxh=120,hh=maxh*Math.min(1,r.utilization/100);ctx.fillStyle='#7c8cff';ctx.beginPath();ctx.roundRect(x,base-hh,bw,hh,6);ctx.fill();ctx.fillStyle=getComputedStyle(document.documentElement).getPropertyValue('--muted');ctx.font='10px system-ui';ctx.textAlign='center';ctx.fillText(r.name,x+bw/2,base+15);ctx.fillText(Math.round(r.utilization)+'%',x+bw/2,base-hh-6)})}
function flowPictorial(){return `<div class="flow-node"><span class="flow-icon">🚑</span><b>Arrivals</b><small class="label">${state.metrics.arrivals_total||0}</small></div><span class="flow-arrow">→</span><div class="flow-node"><span class="flow-icon">🧑‍⚕️</span><b>Queue</b><small class="label">${state.queue_len}</small></div><span class="flow-arrow">→</span><div class="flow-node"><span class="flow-icon">🏥</span><b>Allocated</b><small class="label">${state.in_treatment_count}</small></div><span class="flow-arrow">→</span><div class="flow-node"><span class="flow-icon">✓</span><b>Discharged</b><small class="label">${state.discharged_total}</small></div>`}
async function updateAnalytics(){try{const [a,f,d]=await Promise.all([api('/api/analytics'),api('/api/forecast?horizon=6'),api('/api/departments')]);const r=$('[data-live="analyticsRisk"]'),act=$('[data-live="analyticsActions"]'),flow=$('[data-live="flowPictorial"]'),fp=$('[data-live="forecastPanel"]'),dp=$('[data-live="departmentAnalytics"]');if(r)r.innerHTML=`<b>Capacity pressure</b><div class="risk-meter"><i style="width:${a.capacity_pressure}%"></i></div><div class="label" style="margin-top:7px">${a.capacity_pressure}% pressure • ${a.queue_risk.urgent_waiting} urgent waiting • ${a.queue_risk.avg_wait} avg wait</div><div class="label" style="margin-top:7px">Next-tick expected arrivals: ${a.forecast.expected_arrivals_next_tick}</div>`;if(act)act.innerHTML=`<b>Operational actions</b>${a.recommendations.map(x=>`<div class="action-item">${esc(x)}</div>`).join('')}`;if(flow)flow.innerHTML=flowPictorial();if(fp)fp.innerHTML=`<div class="metric-row"><span>Expected / tick</span><b>${f.expected_arrivals_per_tick[0]}</b></div><div class="metric-row"><span>Next 6 ticks</span><b>${f.expected_total_arrivals}</b></div><div class="metric-row"><span>Current queue</span><b>${f.queue_now}</b></div><div class="label" style="margin-top:9px">${esc(f.method)}</div>`;if(dp)dp.innerHTML=d.map(x=>`<div class="metric-row"><span>${esc(x.department)}</span><b>${x.in_treatment} active • ${x.waiting} waiting</b></div>`).join('');drawLineCanvas($('#queueChart'),state.history.slice(-30).map(x=>x.queue_len),state.history.slice(-30).map(x=>x.avg_resource_utilization));drawResourceCanvas($('#resourceChart'));}catch{}}
function syncLiveUI(){if(!state)return;const set=(sel,html)=>{const el=$(sel);if(el)el.innerHTML=html};set('[data-live="stats"]',commonStats().replace(/^<div class="stats" data-live="stats">|<\/div>$/g,''));$('#heroUtil')?.replaceChildren(document.createTextNode(`${state.metrics.avg_resource_utilization}%`));set('[data-live="resources"]',resourceHTML());set('[data-live="urgency"]',urgencyHTML());set('[data-live="chart"]',chartHTML());set('[data-live="events"]',eventsHTML());set('[data-live="waitingTable"]',`<table><thead><tr><th>ID</th><th>Patient</th><th>Urgency</th><th>Department</th><th>Wait</th><th>Needs</th></tr></thead><tbody>${patientRows(state.waiting,true)}</tbody></table>`);set('[data-live="treatmentTable"]',`<table><thead><tr><th>ID</th><th>Patient</th><th>Urgency</th><th>Department</th><th>Ends</th><th>Resources</th></tr></thead><tbody>${patientRows(state.in_treatment,false)}</tbody></table>`);set('[data-live="departments"]',departmentHTML());set('[data-live="scenarioResources"]',resourceHTML());set('[data-live="scenarioMetrics"]',`<div class="stat"><small>Avg wait</small><strong>${state.metrics.avg_wait_time_treated}</strong></div><div class="stat"><small>Conflicts</small><strong>${state.metrics.capacity_conflicts}</strong></div><div class="stat"><small>Allocations</small><strong>${state.metrics.allocations}</strong></div>`);document.querySelectorAll('[data-strategy]').forEach(b=>b.classList.toggle('active',b.dataset.strategy===state.strategy));const auto=$('#auto');if(auto)auto.textContent=autoTimer?'Stop auto-run':'Start auto-run';updateAnalytics()}
function commonStats(){return `<div class="stats" data-live="stats"><div class="stat"><small>Simulation ticks</small><strong>${state.tick}</strong></div><div class="stat"><small>Waiting queue</small><strong>${state.queue_len}</strong></div><div class="stat"><small>In treatment</small><strong>${state.in_treatment_count}</strong></div><div class="stat"><small>Discharged</small><strong>${state.discharged_total}</strong></div></div>`}
function departmentHTML(){return Object.entries(state.dept_load||{}).map(([name,count])=>`<div class="stat"><small>${esc(name)}</small><strong>${count}</strong><div class="label">patients in treatment</div></div>`).join("")}
function dashboardHTML(){return `<section class="hero reveal reveal-scale"><div><div class="eyebrow">Real-time hospital operations</div><h1>Prioritize patients.<br><span>Optimize resources.</span></h1><p>MEDFLOW models incoming patients, clinical urgency, waiting time, ICU and bed constraints, doctors, nurses, operating rooms and ambulance capacity. Stress it with surges, shortages and failures.</p><div class="hero-actions"><button class="btn primary" id="heroRun">Advance simulation</button><button class="btn" id="heroDemo">Run pressure demo</button></div>${commonStats()}</div><div class="hero-card"><div class="orb"><strong id="heroUtil">${state.metrics.avg_resource_utilization}%</strong></div><p class="label" style="margin-top:14px">Average resource utilization</p><b>${esc(state.strategy_label)}</b></div></section><div class="section-title reveal"><div><h2>Operations dashboard</h2><p>Live resource state and patient-flow metrics.</p></div></div>${controls()}<div class="card reveal" style="margin-top:14px"><div style="display:flex;justify-content:space-between;align-items:center;gap:12px"><div><h3 style="margin-bottom:4px">Decision engine</h3><div class="label">Urgency + waiting time + resource pressure. Explainable scheduling logic from the backend.</div></div><button class="btn" id="decisionBtn">Explain priority queue</button></div><div id="decisionPanel" class="decision-panel" style="margin-top:14px"></div></div><div class="card reveal" style="margin-top:14px"><div style="display:flex;justify-content:space-between;align-items:center;gap:12px"><div><h3 style="margin-bottom:4px">Optimization engine</h3><div class="label">Bounded look-ahead packing: protect urgent cases, reduce starvation, balance utilization, and preserve scarce ICU/OR capacity.</div></div><button class="btn primary" id="optimizeBtn">Build optimal allocation</button></div><div id="optimizationPanel" class="opt-list" style="margin-top:14px"></div></div><div class="grid g2" style="margin-top:14px"><div class="card reveal reveal-left"><h3>Resource utilization</h3><div data-live="resources">${resourceHTML()}</div></div><div class="card reveal reveal-right"><h3>Queue by urgency</h3><div data-live="urgency">${urgencyHTML()}</div></div></div><div class="card reveal" style="margin-top:14px"><h3>Department load</h3><div class="grid g4" data-live="departments">${departmentHTML()}</div></div><div class="grid g2" style="margin-top:14px"><div class="card reveal reveal-left"><h3>Arrival forecast</h3><div data-live="forecastPanel" class="mini-panel"><div class="label">Calculating the next 6 ticks…</div></div></div><div class="card reveal reveal-right"><h3>Department operations</h3><div data-live="departmentAnalytics" class="mini-panel"><div class="label">Live department statistics will appear here.</div></div></div></div><div class="card reveal" style="margin-top:14px"><div class="section-title" style="margin-bottom:10px"><div><h3>Operational intelligence</h3><p>Live graphs and pictorial capacity flow derived from the simulation state.</p></div><div class="label">Updates after every simulation run</div></div><div class="viz-grid"><div class="viz-card"><h4>Queue & utilization</h4><canvas id="queueChart" height="180"></canvas></div><div class="viz-card"><h4>Resource pressure</h4><canvas id="resourceChart" height="180"></canvas></div><div class="viz-card"><h4>Patient flow</h4><div class="flow-pictorial" data-live="flowPictorial"></div></div></div><div class="analytics-grid" style="margin-top:12px"><div class="mini-panel" data-live="analyticsRisk"></div><div class="mini-panel" data-live="analyticsActions"></div></div></div><div class="grid g2" style="margin-top:14px"><div class="card reveal reveal-left"><h3>Wait-time / utilization trend</h3><div class="chart" data-live="chart">${chartHTML()}</div></div><div class="card reveal reveal-right"><h3>Live event stream</h3><div class="events" data-live="events">${eventsHTML()}</div></div></div>`}
function resourceHTML(){return Object.values(state.resources).map(r=>`<div class="resource ${r.overload?'overloaded':''}"><div><b>${esc(r.name)}</b><div class="label">${r.allocated ?? r.in_use}/${r.operational} used ${r.overload?`<span class="overload-badge">OVERLOAD ${r.overload}</span>`:''}</div></div><div><div class="bar"><div class="fill" style="width:${Math.min(100,r.utilization)}%"></div></div><div class="label" style="margin-top:4px">${r.utilization}% ${r.down?`• ${r.down} offline`:''}</div></div><div class="mono" style="text-align:right">${r.available} free</div></div>`).join('')}
function urgencyHTML(){const max=Math.max(1,...Object.values(state.wait_by_urgency));return Object.entries(state.wait_by_urgency).map(([u,n])=>`<div class="resource"><div><span class="chip ${u==1?'critical':u==2?'urgent':u==3?'moderate':'low'}">${URGENCY_LEVELS[u]}</span></div><div class="bar"><div class="fill" style="width:${n/max*100}%"></div></div><b class="mono">${n}</b></div>`).join('')}
function chartHTML(){const h=state.history.slice(-50);if(!h.length)return '<div class="empty">Advance the simulation to generate metrics.</div>';return h.map(x=>`<div class="bar-col" title="Tick ${x.tick}: queue ${x.queue_len}, utilization ${x.avg_resource_utilization}%" style="height:${Math.max(3,Math.min(100,x.avg_resource_utilization))}%"></div>`).join('')}
function eventsHTML(){return state.events.slice(0,35).map(e=>`<div class="event ${esc(e.kind)}"><small class="mono">T${e.tick}</small>${esc(e.message)}</div>`).join('')||'<div class="empty">No events yet.</div>'}
function patientsHTML(){return `<div class="section-title reveal"><div><h2>Patient flow</h2><p>Every patient is assigned only when all required resources are simultaneously available.</p></div><div class="label">${state.queue_len} waiting • ${state.in_treatment_count} active</div></div>${controls()}<div class="card reveal" style="margin-top:14px"><div class="import-zone"><div><b>Import patient CSV</b><div class="label">Required: name, urgency, department. Optional: arrival_time, duration, bed, icu, or, doctor, nurse, ambulance.</div></div><div><input id="patientCsv" type="file" accept=".csv,text/csv" hidden><button class="btn" id="chooseCsv">Choose CSV</button><button class="btn primary" id="uploadCsv">Upload</button><a class="btn" href="/api/patient-template.csv">Template</a></div></div><div id="csvStatus" class="label" style="margin-top:10px">Example: Alice,1,Emergency,4,1,1,0,1,2,1</div></div><div class="grid g2" style="margin-top:14px"><div class="card reveal reveal-left"><h3>Waiting queue</h3><div class="table-wrap" data-live="waitingTable"><table><thead><tr><th>ID</th><th>Patient</th><th>Urgency</th><th>Department</th><th>Wait</th><th>Needs</th></tr></thead><tbody>${patientRows(state.waiting,true)}</tbody></table></div></div><div class="card reveal reveal-right"><h3>In treatment</h3><div class="table-wrap" data-live="treatmentTable"><table><thead><tr><th>ID</th><th>Patient</th><th>Urgency</th><th>Department</th><th>Ends</th><th>Resources</th></tr></thead><tbody>${patientRows(state.in_treatment,false)}</tbody></table></div></div></div>`}
function patientRows(list,waiting){if(!list.length)return '<tr><td colspan="6"><div class="empty">No patients in this list.</div></td></tr>';return list.map(p=>`<tr><td class="mono">#${p.id}</td><td><b>${esc(p.name)}</b>${p.via_ambulance?' 🚑':''}</td><td><span class="chip ${p.urgency==1?'critical':p.urgency==2?'urgent':p.urgency==3?'moderate':'low'}">${esc(p.urgency_label)}</span></td><td>${esc(p.department)}</td><td class="mono">${waiting?state.tick-p.arrival_time:p.end_time}</td><td class="mono">${Object.entries(waiting?p.needs:p.assigned).map(([k,v])=>`${k}:${v}`).join(' ')}</td></tr>`).join('')}
function howToHTML(){return `<div class="howto-head reveal"><div><div class="eyebrow">MEDFLOW guide</div><h1 class="page-title">How to use MEDFLOW</h1><p class="lead">Run a hospital simulation, create operational pressure, watch resources change, and compare scheduling strategies.</p></div></div><div class="guide-grid"><div class="guide-step reveal reveal-left"><span>01</span><h3>Start on Dashboard</h3><p>Use <b>Advance simulation</b> to move one tick, or <b>Advance 10</b> for a faster run.</p></div><div class="guide-step reveal reveal-right"><span>02</span><h3>Try the Demo</h3><p>Press <b>Run pressure demo</b>. It creates a deterministic surge, staff shortage and automatic failures, then starts live simulation.</p></div><div class="guide-step reveal reveal-left"><span>03</span><h3>Inspect Patients</h3><p>Open Patients to see waiting patients, urgency, departments, wait time and resource requirements.</p></div><div class="guide-step reveal reveal-right"><span>04</span><h3>Stress the system</h3><p>Use Scenario Lab to change arrival rates, ambulance pressure, staff shortages and resource failures.</p></div><div class="guide-step reveal reveal-left"><span>05</span><h3>Compare strategies</h3><p>Compare urgency-only scheduling with strategies that also account for waiting time and resource utilization.</p></div><div class="guide-step reveal reveal-right"><span>06</span><h3>Export results</h3><p>Use Export CSV on the Dashboard or Patients page to download the simulation metrics.</p></div></div><div class="card guide-note reveal"><div><b>Tip</b><p>Use the Guided Tour for a step-by-step walkthrough, then experiment with scenarios and compare the resulting patient-flow metrics.</p></div><button class="btn primary" id="howToTour">Start Guided Tour</button></div>`}
function wireHowTo(){$('#howToTour')?.addEventListener('click',startTour)}
function scenariosHTML(){const c=state.config;return `<div class="section-title reveal"><div><h2>Scenario lab</h2><p>Change the operating conditions, then run either the live simulator or an isolated what-if analysis.</p></div></div><div class="grid g2"><div class="card reveal reveal-left"><h3>Live scenario controls</h3>${toggle('surge','Emergency patient surge',state.flags.surge,'Raises arrival pressure by the configured surge multiplier.')}${toggle('staff_shortage','Staff shortage',state.flags.staff_shortage,'Takes a percentage of doctor/nurse capacity offline.')}${toggle('auto_failures','Automatic resource failures',state.flags.auto_failures,'Randomly takes resource units offline during ticks.')}${toggle('icu_constrained','Protect ICU reserve',state.flags.icu_constrained,'Keeps one ICU slot reserved for critical patients when possible.')}<div style="margin-top:18px"><label class="label">Walk-in arrival rate <b id="arv">${c.arrival_rate}</b></label><input id="arrival" type="range" min="0" max="5" step="0.1" value="${c.arrival_rate}"></div><div style="margin-top:18px"><label class="label">Ambulance arrival rate <b id="amv">${c.ambulance_rate}</b></label><input id="ambulance" type="range" min="0" max="3" step="0.05" value="${c.ambulance_rate}"></div><div style="display:flex;gap:8px;margin-top:18px;flex-wrap:wrap"><button class="btn danger" id="failure">Trigger failure</button><button class="btn" id="resolve">Recover failures</button><button class="btn primary" id="runScenario">Run 60-tick what-if</button></div></div><div class="card reveal reveal-right"><h3>Current operating picture</h3><div data-live="scenarioResources">${resourceHTML()}</div><div class="grid g3" style="margin-top:15px" data-live="scenarioMetrics"><div class="stat"><small>Avg wait</small><strong>${state.metrics.avg_wait_time_treated}</strong></div><div class="stat"><small>Conflicts</small><strong>${state.metrics.capacity_conflicts}</strong></div><div class="stat"><small>Allocations</small><strong>${state.metrics.allocations}</strong></div></div><div id="scenarioReport" class="mini-panel" style="margin-top:14px"><div class="label">Run a what-if to see forecast, queue risk and department effects without changing the live simulation.</div></div></div></div>`}
function toggle(id,label,checked,desc){return `<div class="switch-row"><div><b>${label}</b><div class="label">${desc}</div></div><label class="switch"><input id="${id}" type="checkbox" ${checked?'checked':''}><span></span></label></div>`}
function compareHTML(){return `<div class="section-title reveal"><div><h2>Strategy comparison</h2><p>Run the same seed and scenario independently so scheduling strategies are compared on the same arrivals.</p></div></div><div class="card reveal"><div class="grid g3"><div><label class="label">Ticks</label><input id="cmpTicks" type="range" min="20" max="400" step="10" value="120"><div class="mono" id="cmpVal">120</div></div><div><label class="label">Seed</label><input id="cmpSeed" value="42" style="width:100%;padding:10px;border:1px solid var(--line);border-radius:10px;background:var(--surface2);color:var(--text)"></div><div style="display:flex;align-items:end"><button class="btn primary" id="compare" style="width:100%">Run comparison</button></div></div></div><div id="compareResults" class="grid g3 reveal" style="margin-top:14px"></div>`}
const TOUR_STEPS=[
 {title:'Welcome to MEDFLOW',text:'Your live hospital control room: arrivals, queue pressure, resource capacity and patient flow update as the simulation runs.',target:'#heroDemo'},
 {title:'Choose scheduling logic',text:'Switch between urgency-only, waiting-time fairness, utilization-aware scheduling and the multi-objective optimization engine.',target:'#strategy'},
 {title:'Build an allocation plan',text:'The optimization engine evaluates feasible patients using urgency, waiting time, resource scarcity, fragmentation and ICU/OR reserve penalties.',target:'#optimizeBtn'},
 {title:'Advance the simulation',text:'Advance one tick for careful inspection, Advance 10 for a batch, or start auto-run for continuous arrivals and discharges.',target:'#tick1'},
 {title:'Compare strategies',text:'Open Compare to run the same seed and scenario through each scheduler and inspect the resulting operational metrics.',target:'a[href="/compare"]'}
];
let tourIndex=0;
function tourTarget(){return document.querySelector(TOUR_STEPS[tourIndex]?.target)}
function positionTourSpotlight(){const el=tourTarget(),spot=$('#tourSpotlight');if(!el||!spot)return;const r=el.getBoundingClientRect();spot.style.left=`${Math.max(8,r.left-8)}px`;spot.style.top=`${Math.max(8,r.top-8)}px`;spot.style.width=`${Math.max(24,r.width+16)}px`;spot.style.height=`${Math.max(24,r.height+16)}px`;}
function startTour(){if(location.pathname!=='/'&&location.pathname!=='/dashboard'){navigate('/dashboard');setTimeout(startTour,420);return}tourIndex=0;renderTour();$('#tour')?.classList.add('show');$('#tour')?.setAttribute('aria-hidden','false');positionTourSpotlight()}
function closeTour(){ $('#tour')?.classList.remove('show');$('#tour')?.setAttribute('aria-hidden','true');$('#tourSpotlight')?.classList.remove('show');localStorage.setItem('medflow-tour-v2-seen','1') }
function renderTour(){const s=TOUR_STEPS[tourIndex];$('#tourStepNo').textContent=`${tourIndex+1} / ${TOUR_STEPS.length}`;$('#tourTitle').textContent=s.title;$('#tourText').textContent=s.text;$('#tourPrev').disabled=tourIndex===0;$('#tourNext').textContent=tourIndex===TOUR_STEPS.length-1?'Done':'Next';const el=tourTarget();if(el){el.scrollIntoView({behavior:'smooth',block:'center'});setTimeout(()=>{$('#tourSpotlight')?.classList.add('show');positionTourSpotlight()},260)}}
$('#tourBtn')?.addEventListener('click',startTour);$('#tourSkip')?.addEventListener('click',closeTour);$('#tourPrev')?.addEventListener('click',()=>{if(tourIndex>0){tourIndex--;renderTour()}});$('#tourNext')?.addEventListener('click',()=>{if(tourIndex<TOUR_STEPS.length-1){tourIndex++;renderTour()}else closeTour()});$('#tour .tour-backdrop')?.addEventListener('click',closeTour);window.addEventListener('resize',()=>{if($('#tour')?.classList.contains('show'))positionTourSpotlight()});window.addEventListener('scroll',()=>{if($('#tour')?.classList.contains('show'))positionTourSpotlight()},{passive:true});
function wireCommon(){
 document.querySelectorAll('[data-strategy]').forEach(b=>b.onclick=async()=>{
   if(b.dataset.strategy===state.strategy){document.querySelectorAll('[data-strategy]').forEach(x=>x.classList.toggle('active',x===b));return}
   try{setLoading(true);state=await api('/api/strategy',{method:'POST',body:JSON.stringify({strategy:b.dataset.strategy})});document.querySelectorAll('[data-strategy]').forEach(x=>x.classList.toggle('active',x.dataset.strategy===state.strategy));syncLiveUI();toast('Strategy: '+state.strategy_label,'success')}catch(e){toast(e.message,'error')}finally{setLoading(false)}
 });
 $('#tick1')?.addEventListener('click',()=>tick(1));$('#tick10')?.addEventListener('click',()=>tick(10));$('#auto')?.addEventListener('click',toggleAuto);$('#reset')?.addEventListener('click',resetSim);$('#tourInline')?.addEventListener('click',startTour);
}

async function tick(n=1){if(busy)return false;try{setLoading(true);state=await api('/api/tick',{method:'POST',body:JSON.stringify({n})});syncLiveUI();return true}catch(e){if(!String(e.message).includes('Abort'))toast(e.message,'error');return false}finally{setLoading(false)}}
async function autoStep(token=autoRunToken){if(token!==autoRunToken||!autoTimer)return;if(location.pathname!=='/dashboard'){autoTimer=null;syncLiveUI();return}if(busy){autoTimer=setTimeout(()=>autoStep(token),180);return}const ok=await tick(1);if(token===autoRunToken&&autoTimer)autoTimer=setTimeout(()=>autoStep(token),ok?900:350)}
function stopAuto(show=true){autoRunToken++;if(autoTimer){clearTimeout(autoTimer);autoTimer=null}syncLiveUI();if(show)toast('Auto-run paused')}
function toggleAuto(){if(autoTimer){stopAuto(true);return}autoRunToken++;autoTimer=true;syncLiveUI();toast('Auto-run started');autoStep(autoRunToken)}
async function resetSim(){stopAuto(false);try{setLoading(true);state=await api('/api/reset',{method:'POST',body:JSON.stringify({strategy:state.strategy})});renderPage(location.pathname.slice(1)||'dashboard',false);toast('Simulation reset')}catch(e){toast(e.message,'error')}finally{setLoading(false)}}
function wireDashboard(){$('#decisionBtn')?.addEventListener('click',async()=>{const b=$('#decisionBtn'),panel=$('#decisionPanel');b.disabled=true;b.textContent='Calculating…';try{const d=await api('/api/decision',{method:'POST',body:'{}'});panel.innerHTML=d.patients?.length?d.patients.slice(0,8).map(x=>`<div class="decision-row"><b>#${x.patient_id}</b><span>${esc(x.urgency)}</span><span>Wait ${x.wait_time}</span><span>Score ${x.raw_priority}</span></div>`).join(''):'<div class="label">No waiting patients yet.</div>'}catch(e){toast(e.message,'error')}finally{b.disabled=false;b.textContent='Explain priority queue'}});$('#optimizeBtn')?.addEventListener('click',async()=>{const b=$('#optimizeBtn'),panel=$('#optimizationPanel');b.disabled=true;b.textContent='Optimizing…';try{const d=await api('/api/optimize',{method:'POST',body:JSON.stringify({horizon:8})});panel.innerHTML=`<div class="opt-card"><strong>${d.selected_count}</strong><div class="label">patients fit in the current capacity plan</div><div class="label" style="margin-top:8px">Objective: ${esc(d.objective)}</div></div>`+(d.candidates?.slice(0,8).map(x=>`<div class="opt-row"><b>#${x.patient_id} ${esc(x.name)}</b><span>${x.urgency}</span><span>Benefit ${x.benefit}</span><span>${x.feasible?'Feasible':'Blocked'}</span></div>`).join('')||'<div class="label">No waiting patients.</div>');toast(`Optimization evaluated ${d.candidates?.length||0} candidates`,'success')}catch(e){toast(e.message,'error')}finally{b.disabled=false;b.textContent='Build optimal allocation'}});$('#heroRun')?.addEventListener('click',()=>tick(1));$('#heroDemo')?.addEventListener('click',async()=>{const b=$('#heroDemo');if(b.disabled)return;stopAuto(false);b.disabled=true;b.textContent='Preparing demo…';try{setLoading(true);state=await api('/api/demo',{method:'POST',body:'{}'});syncLiveUI();toast('Demo initialized — live hospital pressure is running');b.textContent='Demo running';autoRunToken++;autoTimer=true;autoStep(autoRunToken)}catch(e){toast('Demo failed: '+e.message,'error');b.textContent='Run pressure demo'}finally{setLoading(false);if(b)b.disabled=false}});updateAnalytics()}
function wirePatients(){
 const file=$('#patientCsv'), choose=$('#chooseCsv'), upload=$('#uploadCsv'), status=$('#csvStatus');
 choose?.addEventListener('click',()=>file?.click());
 upload?.addEventListener('click',async()=>{if(!file?.files?.[0]){toast('Choose a CSV file first','error');return}const fd=new FormData();fd.append('file',file.files[0]);upload.disabled=true;upload.textContent='Importing…';try{const r=await fetch('/api/import.csv',{method:'POST',body:fd,cache:'no-store'});const j=await r.json();if(!r.ok||j.ok===false)throw Error(j.error||'CSV import failed');state=j.data.state;status.textContent=`Imported ${j.data.added} patient(s)`+(j.data.errors.length?` • ${j.data.errors.length} row(s) rejected`: '');syncLiveUI();toast(`Imported ${j.data.added} patient(s)`,'success');if(j.data.errors.length)toast(j.data.errors.map(x=>`row ${x.row}: ${x.error}`).join(' • '),'error')}catch(e){status.textContent=e.message;status.style.opacity='1';toast(e.message,'error');clearTimeout(status._hideTimer);status._hideTimer=setTimeout(()=>{status.style.transition='opacity .35s ease';status.style.opacity='0';setTimeout(()=>{status.textContent='';status.style.transition='';},380)},4200)}finally{upload.disabled=false;upload.textContent='Upload'}});
}
function wireScenarios(){
  const bind=(id,key)=>document.querySelector('#'+id)?.addEventListener('change',async e=>{
    try{state=await api('/api/config',{method:'POST',body:JSON.stringify({[key]:e.target.checked})});syncLiveUI();toast('Scenario updated','success')}
    catch(x){toast('Scenario update failed: '+x.message,'error');e.target.checked=!e.target.checked}
  });
  bind('surge','surge');bind('staff_shortage','staff_shortage');bind('auto_failures','auto_failures');bind('icu_constrained','icu_constrained');
  const a=$('#arrival'),m=$('#ambulance');
  a?.addEventListener('input',()=>$('#arv').textContent=a.value);
  a?.addEventListener('change',async()=>{try{state=await api('/api/config',{method:'POST',body:JSON.stringify({arrival_rate:+a.value})});syncLiveUI();toast('Arrival rate updated','success')}catch(e){toast(e.message,'error')}});
  m?.addEventListener('input',()=>$('#amv').textContent=m.value);
  m?.addEventListener('change',async()=>{try{state=await api('/api/config',{method:'POST',body:JSON.stringify({ambulance_rate:+m.value})});syncLiveUI();toast('Ambulance rate updated','success')}catch(e){toast(e.message,'error')}});
  $('#failure')?.addEventListener('click',async()=>{try{state=await api('/api/failure',{method:'POST',body:'{}'});syncLiveUI();toast('Resource failure injected','success')}catch(e){toast(e.message,'error')}});
  $('#resolve')?.addEventListener('click',async()=>{try{state=await api('/api/resolve_failures',{method:'POST',body:'{}'});syncLiveUI();toast('Failures recovered','success')}catch(e){toast(e.message,'error')}});
  $('#runScenario')?.addEventListener('click',async()=>{const b=$('#runScenario'),panel=$('#scenarioReport');b.disabled=true;b.textContent='Simulating…';try{const d=await api('/api/scenario/run',{method:'POST',body:JSON.stringify({ticks:60,seed:state.seed,strategy:state.strategy,surge:state.flags.surge,staff_shortage:state.flags.staff_shortage,auto_failures:state.flags.auto_failures,icu_constrained:state.flags.icu_constrained,arrival_rate:state.config.arrival_rate,ambulance_rate:state.config.ambulance_rate,failure_chance:state.config.failure_chance})});const m=d.summary,a=d.analytics;panel.innerHTML=`<b>60-tick what-if report</b><div class=metric-row><span>Discharged</span><b>${m.patients_discharged}</b></div><div class=metric-row><span>Waiting</span><b>${m.patients_still_waiting}</b></div><div class=metric-row><span>Urgent waiting</span><b>${m.urgent_still_waiting}</b></div><div class=metric-row><span>Avg utilization</span><b>${m.avg_resource_utilization}%</b></div><div class=metric-row><span>Expected arrivals / tick</span><b>${d.forecast.expected_arrivals_per_tick[0]}</b></div><div class=label style=margin-top:9px>${a.recommendations.map(esc).join(' • ')}</div>`;toast('What-if scenario completed','success')}catch(e){toast(e.message,'error')}finally{b.disabled=false;b.textContent='Run 60-tick what-if'}});
}
function wireCompare(){$('#cmpTicks').oninput=e=>$('#cmpVal').textContent=e.target.value;$('#compare').onclick=async()=>{const b=$('#compare');b.disabled=true;b.textContent='Running…';try{const d=await api('/api/compare',{method:'POST',body:JSON.stringify({ticks:+$('#cmpTicks').value,seed:+$('#cmpSeed').value,surge:state.flags.surge,staff_shortage:state.flags.staff_shortage,auto_failures:state.flags.auto_failures,arrival_rate:state.config.arrival_rate,ambulance_rate:state.config.ambulance_rate})});$('#compareResults').innerHTML=d.results.map(x=>`<div class="compare-card"><h4>${esc(x.strategy_label)}</h4><div class="metric-row"><span>Discharged</span><b>${x.patients_discharged}</b></div><div class="metric-row"><span>Still waiting</span><b>${x.patients_still_waiting}</b></div><div class="metric-row"><span>Urgent waiting</span><b>${x.urgent_still_waiting}</b></div><div class="metric-row"><span>Avg treated wait</span><b>${x.avg_wait_time_treated}</b></div><div class="metric-row"><span>Avg utilization</span><b>${x.avg_resource_utilization}%</b></div><div class="metric-row"><span>Capacity conflicts</span><b>${x.capacity_conflicts}</b></div><div class="metric-row"><span>Arrival throughput</span><b>${x.arrival_throughput}/tick</b></div><div class="metric-row"><span>Queue clearance</span><b>${x.queue_clearance_rate}%</b></div></div>`).join('')}catch(e){toast(e.message)}finally{b.disabled=false;b.textContent='Run comparison'}}}

// Initial backend connection + lightweight polling keeps multiple pages synchronized.
(async()=>{await refresh();if(!localStorage.getItem('medflow-tour-v2-seen'))setTimeout(()=>{if(!document.querySelector('.tour.show'))startTour()},700);setInterval(async()=>{if(!busy&&!autoTimer){try{state=await api('/api/state');const p=location.pathname.slice(1)||'dashboard';if(p==='dashboard')syncLiveUI()}catch{}}},3000)})();
</script>
</body>
</html>'''

if __name__ == "__main__":
    port = int(os.environ.get("PORT", "5000"))
    debug = os.environ.get("FLASK_DEBUG", "0") == "1"
    app.run(host="0.0.0.0", port=port, debug=debug, threaded=True)
