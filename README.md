# ⚡ SparkInsight Analytics Platform

✨ **An enterprise-grade big data analytics platform powered by Apache Spark, featuring automated data cleaning, profiling, quality scoring, and AI-generated insights.**

---

✨ **Developed by Team** ✨

1. **T. Sanjay Teja** - 24BDS083
2. **G. Dharmik** - 24BDS021
3. **G. Banu Vardhan Reddy** - 24BDS022
4. **B. Lokeshwara Reddy** - 24BDS011
5. **M. Santhosh** - 24BDS044

---

## 🎥 Project Demonstration & Presentation

**Watch the working demo and presentation video to see SparkInsight Analytics in action:**

👉 [**Click here to watch the Working Demo**](https://drive.google.com/file/d/1vABk1yfG4Oz_SRq2CK3NtdZ84auFTiG9/view?usp=sharing)

👉 [**Click here to watch the Video Presentation**](https://drive.google.com/file/d/1pHEJVCeWoo9dtFG6Byw3UIDIBbN8glf3/view?usp=sharing)

---

## 📌 Overview

The **SparkInsight Analytics Platform** is a high-performance, full-stack distributed data engineering and analytics solution designed to automate traditional data science workflows. Built on top of **Apache Spark**, the platform removes the tedious manual labor of Exploratory Data Analysis (EDA) by seamlessly profiling, cleaning, analyzing, and visualizing massive datasets entirely autonomously.

In today's data-driven world, analysts spend up to 80% of their time cleaning and formatting data. SparkInsight solves this profound bottleneck. Our platform provides a completely secure ecosystem where raw datasets are ingested, analyzed asynchronously utilizing scalable cluster capabilities, and presented back to the user through beautifully formatted, easy-to-read interactive dashboards.

Whether you are dealing with unformatted CSV dumps, missing values, or heavily skewed mathematical distributions, SparkInsight processes everything flawlessly, providing true insights instead of just raw numbers.

### **How It Works (Step-by-Step):**

1. **Secure Login**: Users register and log in safely via Firebase Authentication.
2. **Dataset Selection**: Users browse and select a target dataset from the Explorer interface.
3. **Automated Spark Job**: A heavy-duty asynchronous Apache Spark job kicks off in the background:
   - Evaluates initial data profiling and schema detection.
   - Intelligently applies data cleaning (deduplication, null-handling).
   - Computes a comprehensive Data Quality Score (out of 100).
4. **Insights & Rules**: Core Spark analytics are mapped to generate visual heuristic rules and human-readable AI text insights.
5. **Interactive Dashboard**: Users interact with the final results via a real-time responsive dashboard, complete with dynamic filters and downloadable PDF/Excel exports.

### **Deep Dive: The Autonomous Pipeline Engine**

SparkInsight utilizes a multi-stage distributed processing architecture. Here is exactly how the magic happens behind the scenes:

**1. Secure Authentication & Selection**
The platform features an isolated, token-based boundary via **Firebase Auth**. Users log into their distinct workspace and select raw datasets through the interactive Data Explorer. The selection triggers Firebase to securely tell the backend exactly who initiated the job.

**2. Asynchronous Job Trigger & Architecture**
To prevent timeouts when processing millions of rows, SparkInsight detaches the front-end from the heavy-lifting. The job is queued into a server-side state machine. The frontend establishes a non-blocking UI polling loop to track the pipeline stage progression (`Queued` → `Profiling` → `Cleaning` → `Quality` → `Analytics` → `Completed`) in real-time.

**3. Automated Data Profiling**
A dedicated Apache Spark context mounts the data into memory structures (DataFrames). The engine scans statistical schemas across every single column (detecting Types, Missing Cell Ratios, Unique Boundaries, Skewness, Mean variances) simultaneously without manual configuration.

**4. Intelligent Data Cleaning**
Spark transforms the dataset autonomously:
- Removing exact row duplications.
- Interpreting string anomalies.
- Safely handling `Null` / `NaN` entries so future statistical aggregations do not crash.

**5. Data Quality Scoring Assessment**
A custom algorithm evaluates the scrubbed dataset. It penalizes scores based on column sparseness, duplicate density, and extreme outlies—resulting in a final mathematical **Data Quality Grade** (out of 100).

**6. Analytics & AI Insight Generation**
Spark runs comprehensive MapReduce functions to discover categorical groupings, yearly trends, and numeric distributions. Rather than just returning numbers, our proprietary engine interprets them. It dynamically writes human-readable "insights" (e.g., *"Sales peaked heavily in Q3"*). It also assigns "Heuristic Rules" to seamlessly dictate which mathematical Chart is most appropriate for rendering the results.

**7. Presentation & Dashboarding**
The final, heavily refined JSON payload is transferred back. A meticulously crafted UI layer immediately plots Chart.js graphs, populates data quality rings, and builds comprehensive cross-dataset filters—allowing the user to visually refine the data *post-analysis* and actively download the final reports as polished PDF or Excel files.

---

## 🛠 Tech Stack

* **Frontend:** HTML, Vanilla JavaScript, CSS (Chart.js)
* **Backend:** FastAPI (Python), Apache Spark / PySpark
* **Database & Auth:** Firebase (Admin SDK & Web Auth)
* **Reporting:** ReportLab (PDF), OpenPyXL (Excel)

---

## 🚀 Features

### **Core Data Pipeline**
* 📊 **Automated Profiling** – Fast metadata scanning, null ratio checking, and logic detection.
* 🧹 **Intelligent Data Cleaning** – Deduplication, normalizations, and missing cell handling powered by parallel Spark transformations.
* 💯 **Data Quality Scoring** – Advanced grading algorithms measuring data uniqueness, consistency, completeness, and outlier impacts.
* 📈 **Lightning-Fast Analytics** – Deep statistical computations leveraging heavy Apache Spark memory capacities.

### **Interactive Visuals & "Smart" Layer**
* 🧠 **AI-Generated Insights** – Statistically-driven bullet points translated into plain English.
* 🎯 **Dynamic Filtering** – Smart drop-downs created directly from the scrubbed dataset without requiring recalculations.
* 📉 **Interactive Dashboards** – Chart.js driven visual representations of complex correlations and distributions.
* 📄 **Downloadable Reports** – Single-click exports into PDF and Excel formatting.

### **Enterprise-Grade Architecture**
* 🔐 **Secure Isolation** – Strict user job boundaries locked down via Firebase tokens.
* 🚀 **Asynchronous Execution** – Front-end non-blocking interactions while PySpark handles massive workloads securely in the back.
* 📋 **Job History** – Complete user persistence allowing past report retrieval without recalculation.

---

## 🔧 Installation & Setup

### **Prerequisites**
* Python 3.10+
* Java 8+ (Required for Apache Spark)
* PySpark installed locally
* Firebase Account
* Docker (Optional, recommended)

### **Setup Steps**

1. **Clone Repository**
   ```bash
   git clone https://github.com/SanjayTeja01/BDA-Project.git
   cd BDA-Project
   ```

2. **Firebase Setup**
   - Create a Firebase project at [Firebase Console](https://console.firebase.google.com/)
   - Enable **Authentication (Email/Password)**.
   - Go to Project Settings -> **Service Accounts**, generate a new private key.
   - Rename the downloaded file to `serviceAccountKey.json` and place it in the `backend/` directory.
   - Obtain your **Firebase Web Config** (apiKey, appId, etc.) and update the `FIREBASE_CONFIG` block heavily in both `frontend/login.html` and `frontend/script.js`.

3. **Environment Setup**
   ```bash
   cp .env.example .env
   ```

4. **Install Local Dependencies**
   ```bash
   pip install -r requirements.txt
   ```

5. **Start the Backend server**
   ```bash
   uvicorn backend.main:app --host 0.0.0.0 --port 8000 --reload
   ```

6. **Serve the Frontend website**
   ```bash
   cd frontend/
   python3 -m http.server 3000
   ```
   *⚡ **Application Live URL**: Visit [http://localhost:3000/login.html](http://localhost:3000/login.html) in your browser to start using the platform!*

---

## 🐳 Docker Configuration 

If you prefer a perfectly clean containerized environment, you can run the entire backend using **Docker**.

### **How to Install Docker**
- **Windows:** Download and install [Docker Desktop for Windows](https://docs.docker.com/desktop/install/windows-install/).
- **Mac:** Download and install [Docker Desktop for Mac](https://docs.docker.com/desktop/install/mac-install/).
- **Linux:** Follow official documentation: `sudo apt-get install docker-ce docker-ce-cli containerd.io`

### **Running SparkInsight with Docker Compose**
1. Ensure your local `.env` and `backend/serviceAccountKey.json` files are properly configured safely.
2. Build and start the completely containerized backend services beautifully orchestrated by running:
   ```bash
   docker-compose up --build
   ```
3. The backend API is now running on `http://localhost:8000`. You can serve your static `frontend/` folder locally via standard Python HTTP server (Step 6 above) and interact natively with the app.

---

## 📁 Project Structure

```text
BDA-Project/
├── backend/                  # FastApi and PySpark Logic
│   ├── config/
│   │   └── datasets.json     # Internal catalog mappings
│   ├── analytics_engine.py   # Statistical aggregations
│   ├── auth_middleware.py    # Bearer token security
│   ├── cleaning_pipeline.py  # Spark manipulation and row scrub
│   ├── data_quality_engine.py # Grading score calculator
│   ├── dataset_catalog.py    # Dataset references and loaders
│   ├── dataset_loader.py     # Loads datasets via PySpark memory
│   ├── execution_metrics_engine.py # Diagnostics and metrics tracking
│   ├── filter_engine.py      # Schema-aware filter builder
│   ├── firebase_config.py    # Firebase Admin SDK init
│   ├── insight_generator.py  # AI text generator
│   ├── job_controller.py     # Central pipeline orchestration
│   ├── logger.py             # Error and logic tracking
│   ├── main.py               # Application entrypoint & HTTP endpoints
│   ├── profiling_engine.py   # Initial shape scanning
│   ├── report_generator.py   # Excel/PDF creator
│   ├── result_formatter.py   # JSON merging format layer
│   ├── setup_datasets.py     # Initial dataset preparation 
│   ├── spark_session.py      # Session context factory
│   ├── visualization_engine.py # Chart recommendation layer
│   └── serviceAccountKey.json # SDK Secure Key (Not tracked by Git)
│
├── frontend/                 # Client UI interface 
│   ├── dashboard.html        # Interactive chart presentation layer
│   ├── index.html            # Data Explorer
│   ├── job_history.html      # Persistent report access
│   ├── job_status.html       # Socket-style polling tracker
│   ├── login.html            # Gateway and Firebase Web Auth
│   ├── firebaseConfig.js     # Secure Web API config injected into HTML
│   ├── script.js             # Shared logic and endpoint callers
│   └── style.css             # System-wide dark styling
│
├── logs/                     # Application execution logs
├── docker-compose.yml        # Advanced container orchestration
├── Dockerfile                # Environment container instructions
├── README.md                 # System overview
├── requirements.txt          # Python strict dependencies
├── .env                      # Hidden Environment Variables
└── .gitignore                # Git Tracking Ignore File
```

---

## 🗄️ Database Architecture

### **Collections (Firestore):**

**`users/{uid}`** (Logical boundaries set by Google Cloud IAM)
- Subcollections:
  - `jobs/{job_id}` - Contains dataset target, `start_time`, error state tracking, and retry sequences.
  - `job_results/{job_id}` - The massive final JSON outcome containing profiles, metrics, clean statuses, charts, and generated insights.

---

## 🚀 Future Improvements

* Streaming data ingestion support.
* Fully standalone cloud worker microservices.
* Custom user dataset uploading via CSV parsing.
* Implementation of advanced Machine Learning models (Predictive Analysis).
* Multi-tenant institutional RBAC structures.

---

## 📝 Important Notes

1. **Authentication:** Backend explicitly requires headers sent matching `Authorization: Bearer <token>`.
2. **CORS:** Ensure your frontend URL is allowlisted in `main.py` if running outside `localhost:3000`.
3. **Hardware Constraints:** Spark utilizes significant RAM based on dataset sizing.

---

## 🙏 Acknowledgments

* Apache Software Foundation for PySpark backend heavy-lifting.
* Google Firebase for instantaneous serverless components.
* Chart.js for beautiful browser-based UI rendering.

---

**Happy Analyzing! ⚡**
