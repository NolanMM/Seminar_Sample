# Project Introduction: AI-Powered Financial Data Pipeline & Visualization

## I. Project Introduction
🚀 Welcome to the AI-Powered Financial Data Pipeline & Visualization project! 🚀 This repository illustrates how to design, build, and deploy a comprehensive system that integrates artificial intelligence models with a robust and efficient data pipeline. The project leverages state-of-the-art technologies to process, analyze, and visualize financial data for insightful decision-making.

---

## VI. Project Architectures

---

## II. Tech Stack
1. **Data Collection & Preprocessing**  
   - **Python Libraries:** `polars`, `requests`  
   - **APIs:** [Finhub API](https://finnhub.io/), [Financial Modeling Prep API](https://financialmodelingprep.com/)

2. **Data Storage**  
   - **SQL Database:** `PostgreSQL` for structured data storage and retrieval  
   - **Cloud Storage:** `Google Cloud Storage` for raw data and AI model artifacts  

3. **Data Analysis & AI Modeling**  
   - **Exploratory Data Analysis (EDA):** `polars`, `Plotly`  
   - **Feature Engineering & Transformation:** `polars`, custom preprocessing pipelines  
   - **Models:**  
     - Financial Forecasting Model: Developed with `PyTorch` and optimized/exported using `ONNX`  

4. **Workflow Orchestration**  
   - **Tool:** `Prefect v2.0` for robust pipeline orchestration and task scheduling  

5. **Visualization & User Interface**  
   - **Interactive Visualization:** `Plotly` for dynamic and interactive financial data visualizations  
   - **Web Framework:** `Streamlit` for building user-friendly dashboards and interfaces  

6. **Model Evaluation**  
   - **Metrics:** R-squared, Mean Squared Error (MSE), and custom financial metrics  
   - **Tools:** `PyTorch`, `ONNX` for evaluation and deployment-ready models  

---

## III. Prerequisites

Before starting, ensure your environment is set up with the required tools and libraries. Follow these steps to install and configure the necessary prerequisites:

### 1. **Python 3.12**

- Ensure you have Python 3.12 installed. You can download it from the [official Python website](https://www.python.org/downloads/).  
- Verify the installation:
  ```bash
  python3 --version
    ```

<details><summary>Instructions to create a new `Python 3.10` virtual environment if not available.</summary>

* To create a new virtual environment, use the following command in the terminal of the project directory:

  * In Windows or Linux, use:
  
  ```bash
  python -m venv venv
  ```

  * Activate the virtual environment with:
  
  ```bash
  venv\Scripts\activate
  ```

  * In macOS, use:
  
  ```bash
  python3 -m venv venv
  ```

  * Activate the virtual environment with:
  
  ```bash
  source venv/bin/activate
  ```

* Ensure the virtual environment is activated in the corresponding project directory:

  * In Windows or Linux:
  
  ```bash
  venv\Scripts\activate
  ```

  * In macOS:
  
  ```bash
  source venv/bin/activate
  ```

* Install dependencies from `requirements.txt`:
  
  ```bash
  pip install -r requirements.txt
  ```

</details>

### 2. PyTorch Installation

<details>

<summary><b>Set Up</b></summary>

- Install PyTorch using the appropriate version for your system. [Check the official PyTorch website]() for the latest installation instructions.

- For example, for systems with CUDA 11.8:
    ```bash
  pip install torch torchvision torchaudio --index-url https://download.pytorch.org/whl/cu118
    ```
- Verify the installation by type in command line:
    ```bash
    python -c "import torch; print(torch.__version__)"
    ```

- Checking CUDA Compatibility
    - Check if your system supports CUDA:
        ```bash
        nvidia-smi
        ```
        If GPU details appear, your system is CUDA-compatible.
        <br>
    - Install CUDA if needed:
        - Download the CUDA Toolkit from the [NVIDIA CUDA website]() and follow the installation guide for your OS.
        - Verify the CUDA version:
            ```bash
            nvcc --version
            ```

</details>

### 3. PostgreSQL 16
- Install PostgreSQL 16. Follow the instructions for your operating system from the [Official PostgreSQL website]().

- Verify the installation:
    ```bash
    psql --version
    ```
    The output should display psql (PostgreSQL) 16.x.

---

## IV. Installation

1. Clone the Git Repository
    ```bash
    ```
2. Create Python Virtual Environment
3. Installed Dependencies that needed for the project
    ```bash
    pip install -r requirements.txt
    ```
4. Input the Key Information into

## VI. Usage


