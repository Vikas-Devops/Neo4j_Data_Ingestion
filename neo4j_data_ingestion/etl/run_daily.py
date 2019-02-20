from mailer import Mailer
from logger import Logger
from organization import Organization
from person import Person 
from fund import Fund
from funding import Funding
from fundingRound import FundingRound
from hasAcquired import HasAcquired
from hasAffiliation import HasAffiliation
from hasFund import HasFund
from investment import Investment
from hasStudiedat import HasStudiedat
from apscheduler.schedulers.blocking import BlockingScheduler

__author__ = "Jagjeet Goraya"
__version__ = "0.1"
__status__ = "development"

log_file = 'run_daily.py'
sched = BlockingScheduler()

@sched.scheduled_job('cron', day_of_week='mon-sun', hour=21, minute=0)
def run_process():
    """."""

    
    try:
        
        Organization.run()
        Fund.run()
        Person.run()
        FundingRound.run()
        HasAcquired.run()
        Funding.run()
        HasFund.run()
        Investment.run()
        HasAffiliation.run()
        HasStudiedat.run()

    except Exception as e:
        logger = Logger()
        logger.logg(debug_msg = 'Error while running cron job.',
            info_msg = 'Function = run_process()',
            warning_msg = 'Error in executing scheduled job',
            error_msg = 'Module = '+log_file, 
            critical_msg = str(e))

run_process()
# sched.start()



