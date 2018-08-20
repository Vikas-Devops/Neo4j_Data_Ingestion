from database import Database
from mailer import Mailer
from company import Company
from person import Person 
from fund import Fund
from funding import Funding
from fundingRound import FundingRound
from hasAcquired import HasAcquired
from hasAffiliation import HasAffiliation
from hasFund import HasFund
from investment import Investment
from apscheduler.schedulers.blocking import BlockingScheduler

__author__ = "Jagjeet Goraya"
__version__ = "0.1"
__status__ = "development"

sched = BlockingScheduler()

@sched.scheduled_job('cron', day_of_week='mon-sun', hour=5, minute=30)
def run_process():
    """."""
    
    try:
        Company.run()
        FundingRound.run()
        Fund.run()
        HasAcquired.run()
        Funding.run()
        HasFund.run()
        Person.run()
        Investment.run()
        HasAffiliation.run()

    except Exception as e:
        print e

sched.start()



