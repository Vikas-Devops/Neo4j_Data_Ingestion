from mailer import Mailer
from logger import Logger
from category import Category
from event import Event 
from hasCBCategory import HasCBCategory
from hasParticipant import HasParticipant
from apscheduler.schedulers.blocking import BlockingScheduler

__author__ = "Jagjeet Goraya"
__version__ = "0.1"
__status__ = "development"

log_file = 'run_weekly.py'
sched = BlockingScheduler()


@sched.scheduled_job('cron', day_of_week='fri', hour=22, minute=0)
def run_process():
    """."""

    try:
        Event.run()
        HasParticipant.run()
        Category.run()
        HasCBCategory.run()

    except Exception as e:
        logger = Logger()
        logger.logg(debug_msg = 'Error while running cron job.',
            info_msg = 'Function = run_process()',
            warning_msg = 'Error in executing scheduled job',
            error_msg = 'Module = '+log_file, 
            critical_msg = str(e))
        
sched.start()



