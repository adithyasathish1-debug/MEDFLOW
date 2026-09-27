MEDFLOW — Hospital Resource Management Simulator
MEDFLOW is a real-time hospital resource management and patient triage simulator built with Flask. It models emergency/walk-in arrivals, clinical urgency tiers, waiting-time fairness, staff shortages, emergency patient surges, and multi-resource capacity allocation (beds, ICUs, operating rooms, doctors, nurses, and ambulances).
Features
Real-Time Simulation Ticks: Advance the hospital clock tick-by-tick or start an auto-run stream.
Multiple Scheduling Strategies:
Urgency Only: Pure triage based strictly on patient acuity.
Urgency + Wait Time: Prevents starvation by factoring in cumulative waiting time.
Urgency + Wait + Utilization: Balances scheduling against live resource pressure.
Optimization Engine: Bounded beam-search look-ahead model evaluating multiple constraints and penalties.
Stress Testing & Scenarios: Inject sudden patient surges, staff shortages, automatic resource failures, and critical ICU/OR reserve constraints.
Data Import & Export: Upload custom CSV patient batches or export simulation metrics for external analysis.
Interactive Dashboard: Live charts, department load metrics, event feeds, and an integrated guided tour.
Local Installation & Running
Clone the Repository & Navigate to Project Directory
git clone <your-github-repo-url>
cd <repository-folder>


Install Dependencies
Make sure you have Python 3.8+ installed, then install Flask:
pip install Flask


Run the Application
python "app (23).py"

(Note: You can rename the file to app.py for easier deployment).
Open in Browser
Navigate to http://127.0.0.1:5000.
Deploying to Vercel via GitHub
Vercel supports Python Serverless Functions through its WSGI/ASGI adapters. To host this single-file application on Vercel:
Prepare your GitHub Repository:
Rename your main application file to index.py or keep it as app (23).py and reference it appropriately. (For standard Vercel Python templates, placing the WSGI app inside an api/ directory or root configuration works seamlessly).
Ensure you have a requirements.txt file containing:
Flask==3.0.2


Add the vercel.json file provided in this repository to your root folder.
Connect to Vercel:
Push your code to your GitHub repository.
Log in to Vercel, click Add New... > Project, and import your GitHub repository.
Vercel will automatically detect the configuration and deploy your Flask app as a serverless function.
