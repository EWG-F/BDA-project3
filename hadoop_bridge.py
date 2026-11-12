"""
TrendScope - Hadoop & Big Data Bridge Module
Bridges Streamlit application with Hadoop HDFS, MapReduce Streaming, and Apache Hive.
Supports both Live Hadoop Execution and Standalone Demo Mode.
"""

import glob
import os
import shlex
import subprocess
import pandas as pd

try:  # imported as `src.hadoop_bridge`
    from .paths import (
        BASE_DIR,
        DATA_PROCESSED_DIR,
        DATA_RAW_DIR,
        HIVE_DIR,
        MAPREDUCE_DIR,
        resolve,
    )
except ImportError:  # imported as flat `hadoop_bridge` (flattened repo)
    from paths import (
        BASE_DIR,
        DATA_PROCESSED_DIR,
        DATA_RAW_DIR,
        HIVE_DIR,
        MAPREDUCE_DIR,
        resolve,
    )

# One entry per streaming job: its folder under mapreduce/, the raw TSV it
# writes, and the aggregate CSV the dashboard reads.
JOB_SCRIPTS = {
    1: {
        "job_dir": "job1_keyword_frequency",
        "tsv": "mr_job1_raw_output.tsv",
        "csv": "mapreduce_job1_keywords.csv",
    },
    2: {
        "job_dir": "job2_category_stats",
        "tsv": "mr_job2_raw_output.tsv",
        "csv": "mapreduce_job2_category_stats.csv",
    },
    3: {
        "job_dir": "job3_trending_persistence",
        "tsv": "mr_job3_raw_output.tsv",
        "csv": "mapreduce_job3_persistence.csv",
    },
}


def job_script_paths(job_number):
    """Absolute paths to the mapper/reducer pair for a job."""
    job_dir = JOB_SCRIPTS[job_number]["job_dir"]
    folder = os.path.join(MAPREDUCE_DIR, job_dir)
    mapper = resolve(folder, "mapper.py")
    reducer = resolve(folder, "reducer.py")
    return mapper, reducer


class HadoopBridge:
    def __init__(self):
        self.hadoop_home = os.environ.get("HADOOP_HOME", "")
        self.hive_home = os.environ.get("HIVE_HOME", "")
        self.base_dir = BASE_DIR
        self.mapreduce_dir = MAPREDUCE_DIR
        self.hive_dir = HIVE_DIR
        self.processed_dir = DATA_PROCESSED_DIR
        self.raw_dir = DATA_RAW_DIR

    def check_hadoop_installed(self):
        """
        Checks whether Hadoop and HDFS CLIs are available on the operating system.
        """
        try:
            res = subprocess.run(["hadoop", "version"], capture_output=True, text=True, timeout=5)
            return res.returncode == 0
        except Exception:
            return False

    def check_hive_installed(self):
        """
        Checks whether Apache Hive is available in PATH.
        """
        try:
            res = subprocess.run(["hive", "--version"], capture_output=True, text=True, timeout=5)
            return res.returncode == 0
        except Exception:
            return False

    def run_mapreduce_job_locally(self, job_number=1):
        """
        Executes a specific MapReduce job using local Python standard I/O streaming pipes.
        """
        if job_number not in JOB_SCRIPTS:
            return False, "Invalid Job Number"

        out_filename = JOB_SCRIPTS[job_number]["tsv"]
        mapper, reducer = job_script_paths(job_number)
        out_file = os.path.join(DATA_PROCESSED_DIR, out_filename)

        if not os.path.exists(mapper) or not os.path.exists(reducer):
            return False, f"Mapper/reducer for Job {job_number} not found in {self.mapreduce_dir}"

        raw_files = sorted(glob.glob(os.path.join(self.raw_dir, "*videos.csv")))
        if not raw_files:
            return False, f"No *videos.csv input found in {self.raw_dir}"

        os.makedirs(DATA_PROCESSED_DIR, exist_ok=True)

        try:
            # cat raw/*videos.csv | python3 mapper.py | sort | python3 reducer.py > out_file
            cmd = "cat {sources} | python3 {mapper} | sort | python3 {reducer} > {out}".format(
                sources=" ".join(shlex.quote(f) for f in raw_files),
                mapper=shlex.quote(mapper),
                reducer=shlex.quote(reducer),
                out=shlex.quote(out_file),
            )
            res = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=120)
            if res.returncode == 0:
                # A TSV output and a CSV aggregate are both written by the pipeline scripts.
                return True, f"MapReduce Job {job_number} executed successfully! Output saved to {out_filename}"
            else:
                return False, f"MapReduce Job {job_number} error: {res.stderr.strip() or 'non-zero exit'}"
        except Exception as e:
            return False, f"Execution failed: {str(e)}"

    def get_job_output_dataframe(self, job_number=1):
        """
        Loads the output dataset for the requested MapReduce job.
        """
        if job_number not in JOB_SCRIPTS:
            return pd.DataFrame()

        csv_filename = JOB_SCRIPTS[job_number]["csv"]
        path = resolve(self.processed_dir, csv_filename)
        if not os.path.exists(path):
            return pd.DataFrame()
        try:
            return pd.read_csv(path)
        except Exception:
            return pd.DataFrame()

    def get_hive_query_script(self):
        """
        Reads analytical HiveQL script text.
        """
        hql_path = resolve(self.hive_dir, "analytical_queries.hql")
        if os.path.exists(hql_path):
            with open(hql_path, 'r', encoding='utf-8') as f:
                return f.read()
        return "-- HiveQL script not found"

    def get_cluster_status(self):
        """
        Returns environment diagnostic parameters.
        """
        hadoop_avail = self.check_hadoop_installed()
        hive_avail = self.check_hive_installed()
        
        return {
            "hadoop_available": hadoop_avail,
            "hive_available": hive_avail,
            "hdfs_namenode": "hdfs://localhost:9000" if hadoop_avail else "Standalone Virtual HDFS (Demo)",
            "hive_metastore": "thrift://localhost:9083" if hive_avail else "Embedded Metastore (Precomputed)",
            "execution_engine": "MapReduce 3.3.6 & Hive on Tez" if hadoop_avail else "Python Hadoop Streaming & Hive Engine"
        }
