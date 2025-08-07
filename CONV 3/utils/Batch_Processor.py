
import time
import logging
import subprocess
from pathlib import Path
import sys

# Log format
log_format = '%(asctime)s | %(levelname)s | %(message)s'

# File handler (UTF-8, supports emojis)
file_handler = logging.FileHandler("batch_processing.log", encoding='utf-8')
file_handler.setFormatter(logging.Formatter(log_format))

# Console handler (no emojis)
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setFormatter(logging.Formatter(log_format))

# Logger configuration
logging.basicConfig(
    level=logging.DEBUG,
    handlers=[file_handler, console_handler]
)

logger = logging.getLogger(__name__)

class BatchProcessor:
    def __init__(self, file_paths: list[str]):
        base_path = r"C:\Users\US171177\Projects\Unitil\Go Anywhere\CONV 2B"
        self.file_paths = generate_file_paths(base_path)
        self.results = []

    def run_all(self):
        logger.info("🚀 Starting batch processing...\n" + "=" * 60)

        for i, file_path in enumerate(self.file_paths, start=1):
            logger.info("\n" + "-" * 60)
            logger.info(f"🔹 Batch {i}/{len(self.file_paths)}")
            logger.info(f"📄 Running file: {file_path}")
            logger.info("-" * 60)

            result = self.run_file(Path(file_path))
            self.results.append(result)

        logger.info("\n✅ Batch processing completed.")
        logger.info("=" * 60 + "\n")
        self.log_summary()

    def run_file(self, file_path: Path):
        start_time = time.time()
        status = "Success"
        error_message = ""

        try:
            result = subprocess.run(
                ["python", str(file_path)],
                capture_output=True,
                text=True,
                check=True
            )
            logger.info(f"{file_path.name} executed successfully.")
            if result.stdout:
                logger.info(f"Output:\n{result.stdout.strip()}")
            if result.stderr:
                logger.warning(f"Warnings:\n{result.stderr.strip()}")

        except subprocess.CalledProcessError as e:
            status = "Failed"
            error_message = e.stderr.strip() if e.stderr else str(e)
            logger.error(f"Error running {file_path.name}: {error_message}")
            if e.stdout:
                logger.error(f"Stdout:\n{e.stdout.strip()}")

        end_time = time.time()
        duration = round(end_time - start_time, 2)

        return {
            "file": str(file_path),
            "status": status,
            "duration": duration,
            "error": error_message
        }

    def log_summary(self):
        total = len(self.results)
        passed = sum(1 for r in self.results if r['status'] == "Success")
        failed = total - passed

        logger.info("\n📋 Batch Summary")
        logger.info("=" * 100)
        logger.info(f"{'Batch#':<7} | {'File':<45} | {'Status':<10} | {'Time (s)':<8} | Error")
        logger.info("-" * 100)

        for idx, result in enumerate(self.results, start=1):
            file_name = Path(result['file']).name
            status_icon = "✅" if result['status'] == "Success" else "❌"
            status_text = f"{status_icon} {result['status']}"
            duration = f"{result['duration']:.2f}"

            error_msg = result["error"]
            if error_msg:
                error_msg = error_msg.replace('\n', ' ').replace('\r', '')
                error_msg = (error_msg[:40] + "...") if len(error_msg) > 43 else error_msg
            else:
                error_msg = ""

            logger.info(f"{idx:<7} | {file_name:<45} | {status_text:<10} | {duration:<8} | {error_msg}")

        logger.info("-" * 100)
        logger.info(f"🧾 Total Scripts: {total} | ✅ Passed: {passed} | ❌ Failed: {failed}")
        logger.info("=" * 100 + "\n")


def generate_file_paths(base_path: str) -> list[str]:
    # Ordered list of tuples: (group, filename)
    ordered_files = [
        ("Group A", "STAGE_STREETS.py"),
        ("Group A", "STAGE_CYCLE.py"),
        ("Group A", "STAGE_ROUTE.py"),
        ("Group A", "STAGE_TOWNS.py"),
        ("Group A", "STAGE_DEVICE.py"),
        ("Group A", "STAGE_CUST_INFO.py"),
        ("Group A", "STAGE_MAIL_ADDR.py"),
        ("Group A", "STAGE_PREMISE.py"),
        ("Group A", "STAGE_SSN_LICENSE.py"),
        ("Group A", "STAGE_PHONE.py"),
        ("Group A", "STAGE_EMAIL.py"),
        ("Group A", "STAGE_BILLING_ACCT.py"),
        ("Group A", "STAGE_REPORT_CODES.py"),

        ("Group B", "STAGE_METERED_SVCS.py"),
        ("Group B", "STAGE_FLAT_SVCS.py"),
        ("Group B", "STAGE_UNBILLED_READINGS.py"),
        ("Group B", "WRITE_OFF_BALANCES.py"),
        ("Group B", "STAGE_AR_BALANCES.py"),
        ("Group B", "STAGE_DEPOSITS.py"),

        ("Group C", "STAGE_CONSUMPTION_HIST.py"),
        ("Group C", "STAGE_TRANSACTIONAL_HIST.py"),
        ("Group C", "STAGE_CUST_NOTES.py"),

        ("Group D", "STAGE_BUDGET.py"),
        ("Group D", "STAGE_TAX_EXEMPTIONS.py"),

        ("Group P", "STAGE_BILL_TRANS.py"),
        ("Group P", "STAGE_CHARGE_MAP.py"),

        ("Group M", "STAGE_METER_INVENTORY.py"),
    ]

    # Build full paths
    full_paths = [str(Path(base_path) / group / filename) for group, filename in ordered_files]
    return full_paths


base_path = r"C:\Users\US171177\Projects\Unitil\Go Anywhere\CONV 2B"
files_to_process = generate_file_paths(base_path)
# Create an instance of BatchProcessor
batch = BatchProcessor(file_paths=files_to_process)

# Run all processing
batch.run_all()