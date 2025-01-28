import sqlite3
import random
import datetime

def create_database():
    """Creates a SQLite database with a table for generated jobs."""
    conn = sqlite3.connect('jobs.db')
    cursor = conn.cursor()

    # Create table if not exists
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS generated_jobs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        job_id TEXT,
        partition TEXT,
        account TEXT,
        user TEXT,
        nnodes INTEGER,
        ncpus INTEGER,
        cpu_takt FLOAT,
        io_usage FLOAT,
        memory_usage FLOAT,
        data_input_size FLOAT,
        data_output_size FLOAT,
        submit_time TEXT,
        start_time TEXT,
        end_time TEXT,
        elapsed_time INTEGER,
        mips_estimate FLOAT
    )
    ''')
    conn.commit()
    conn.close()

def generate_hpc_jobs(n):
    """Generates n random job entries with realistic HPC parameters and inserts them into the database."""
    conn = sqlite3.connect('jobs.db')
    cursor = conn.cursor()

    partitions = ['debug', 'normal', 'gpu']
    accounts = ['research', 'engineering', 'bio']
    users = ['user1', 'user2', 'user3']
    BASE_CPU_TAKT = 2.45  # Fixed CPU clock speed in GHz for all jobs
    SCALING_FACTOR = 0.25  # Scaling factor to simulate realistic efficiency

    for _ in range(n):
        job_id = f"job_{random.randint(1000, 9999)}"
        partition = random.choice(partitions)
        account = random.choice(accounts)
        user = random.choice(users)
        nnodes = random.randint(1, 16)  # More realistic node count for HPC jobs
        ncpus = nnodes * random.randint(16, 64)  # CPUs per node (e.g., 16-64 cores per node)

        # Simulated job characteristics
        io_usage = round(random.uniform(0.1, 10.0), 2)  # Simulated I/O usage in GB/s
        memory_usage = round(random.uniform(4.0, 1024.0), 2)  # Memory usage in GB (realistic range for HPC)
        data_input_size = round(random.uniform(10.0, 1000.0), 2)  # Input data size in GB
        data_output_size = round(random.uniform(1.0, 500.0), 2)  # Output data size in GB

        # Generate realistic job durations
        submit_time = datetime.datetime.now() - datetime.timedelta(hours=random.randint(1, 48))
        start_time = submit_time + datetime.timedelta(minutes=random.randint(1, 60))
        elapsed_time = random.randint(300, 86400)  # Runtime in seconds (5 minutes to 24 hours)
        end_time = start_time + datetime.timedelta(seconds=elapsed_time)

        # Estimate MIPS based on a more realistic heuristic
        mips_estimate = ncpus * BASE_CPU_TAKT * 1e6 * SCALING_FACTOR  # Simulate realistic MIPS

        # Insert job into the database
        cursor.execute('''
        INSERT INTO generated_jobs (job_id, partition, account, user, nnodes, ncpus, cpu_takt, io_usage, memory_usage, data_input_size, data_output_size, submit_time, start_time, end_time, elapsed_time, mips_estimate)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (job_id, partition, account, user, nnodes, ncpus, BASE_CPU_TAKT, io_usage, memory_usage, data_input_size, data_output_size, submit_time.isoformat(), start_time.isoformat(), end_time.isoformat(), elapsed_time, mips_estimate))

    conn.commit()
    conn.close()

if __name__ == "__main__":
    # Create database and table
    create_database()

    # Generate realistic HPC job data
    generate_hpc_jobs(10000)
    print("Generated 10,000 realistic HPC job entries.")
