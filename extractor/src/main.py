from utils.orchestrator import Orchestrator
from utils.logger import setup_logging



# Logging setup
logger = setup_logging("Main")

def main():
    """Main function to run the ETL process."""
    logger.info("Starting ETL process...")
    orchestrator = Orchestrator()
    orchestrator.process_orchestration()
    logger.info("ETL process completed.")



if __name__ == "__main__":
    main()
