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

## 🎥 Project Working Demo

**Watch the demo video to see SparkInsight Analytics in action:**

👉 [**Click here to watch the demo video**](#) *(Add your presentation link here)*

---

## 📌 Overview

**SparkInsight Analytics Platform** is a full-stack, distributed data analytics application designed to help businesses seamlessly profile, clean, analyze, and visualize massive datasets. 

### **How It Works:**

1. Users register and log securely via Firebase Authentication.
2. Users select a target dataset from the Explorer interface.
3. Upon analysis trigger, a heavy-duty asynchronous Apache Spark job kicks off:
   - Evaluates initial data profiling and schema detection.
   - Intelligently applies data cleaning (deduplication, null-handling).
   - Computes a comprehensive Data Quality Score out of 100.
4. Core Spark analytics are aggregated to generate visual heuristic rules and human-readable text insights.
5. Users interact with the final results via a real-time responsive dashboard, complete with dynamic filters and downloadable PDF/Excel reports.

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
   cd sparkinsight
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
   *Visit `http://localhost:3000/login.html` in your browser.*

---

## 🐳 Docker Configuration 

If you prefer a perfectly clean containerized environment, you can run the entire backend using **Docker**.

### **How to Install Docker**
- **Windows:** Download and install [Docker Desktop for Windows](https://docs.docker.com/desktop/install/windows-install/).
- **Mac:** Download and install [Docker Desktop for Mac](https://docs.docker.com/desktop/install/mac-install/).
- **Linux:** Follow official documentation: `sudo apt-get install docker-ce docker-ce-cli containerd.io`

### **Running SparkInsight with Docker**
1. Ensure your `backend/serviceAccountKey.json` is present.
2. Build the Docker Image:
   ```bash
   docker build -t sparkinsight .
   ```
3. Run the Container securely binding it to your local port 8000:
   ```bash
   docker run -p 8000:8000 sparkinsight
   ```
4. Once the Docker container says it's running, you can serve your static `frontend/` folder locally via standard Python HTTP server (Step 6 above) and interact natively with the app.

---

## 📁 Project Structure

```
sparkinsight/
├── backend/                  # FastApi and PySpark Logic
│   ├── config/
│   │   └── datasets.json     # Internal catalog mappings
│   ├── main.py               # Application entrypoint & HTTP endpoints
│   ├── firebase_config.py    # Firebase Admin SDK init
│   ├── auth_middleware.py    # Bearer token security
│   ├── job_controller.py     # Central pipeline orchestration
│   ├── dataset_loader.py     # Loads datasets via PySpark memory
│   ├── profiling_engine.py   # Initial shape scanning
│   ├── cleaning_pipeline.py  # Spark manipulation and row scrub
│   ├── data_quality_engine.py # Grading score calculator
│   ├── analytics_engine.py   # Statistical aggregations
│   ├── execution_metrics_engine.py # Diagnostics and metrics tracking
│   ├── filter_engine.py      # Schema-aware filter builder
│   ├── visualization_engine.py # Chart recommendation layer
│   ├── insight_generator.py  # AI text generator
│   ├── result_formatter.py   # JSON merging format layer
│   ├── report_generator.py   # Excel/PDF creator
│   ├── spark_session.py      # Session context factory
│   └── logger.py             # Error and logic tracking
│
├── frontend/                 # Client UI interface 
│   ├── login.html            # Gateway and Firebase Web Auth
│   ├── index.html            # Data Explorer
│   ├── dashboard.html        # Interactive chart presentation layer
│   ├── job_status.html       # Socket-style polling tracker
│   ├── job_history.html      # Persistent report access
│   ├── script.js             # Shared logic and endpoint callers
│   └── style.css             # System-wide dark styling
│
├── Dockerfile                # Environment container instructions
├── docker-compose.yml        # Advanced container orchestration
├── README.md                 # System overview
└── requirements.txt          # Python strict dependencies
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
