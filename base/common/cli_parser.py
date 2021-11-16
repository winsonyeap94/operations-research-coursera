import os
import sys
import argparse
import getpass
from platform import uname
from datetime import datetime
from sqlalchemy import create_engine
from collections import OrderedDict

from .logger import Logger

# ========== Parser Settings ==========
__sys_version__ = sys.version_info
__date_format__ = "%Y-%m-%d %H:%M:%S"
try:
    __user__ = getpass.getuser()
except:
    __user__ = 'pipeline'
__sys_name__ = uname()[1]


class BaseParser(object):

    @staticmethod
    def is_valid_file(self, parser, arg):
        if arg != '' and not os.path.exists(arg):
            parser.error("The file %s does not exist!" % arg)
        else:
            return open(arg, 'r')  # return an open file handle

    @staticmethod
    def restricted_float(self, x):
        frange = (0, 1)
        x = float(x)
        if x < frange[0] or x > frange[1]:
            raise argparse.ArgumentTypeError("%r not in range [{}, {}}]" % (frange))
        return x

    def get_vars(self, ignore_list=[]):
        tasks = self.parser._get_positional_actions()[0].choices
        options = {}
        for key, task in tasks.items():
            options[key] = OrderedDict()
            for action in task._actions:
                pars = vars(action)
                if pars['dest'] not in ['help', 'version'] + ignore_list:
                    options[key][pars['dest']] = {
                        p: pars[p] for p in pars if p not in ["dest", 'metavar', 'container']}
                    options[key][pars['dest']]['general'] = False

        # General options
        for task in options:
            for action in self.parser._actions:
                if type(action) in [argparse._StoreTrueAction, argparse._StoreAction]:
                    pars = vars(action)
                    if not pars['dest'] in ignore_list:
                        options[task][pars['dest']] = {
                            p: pars[p] for p in pars if p not in ["dest", 'metavar', 'container']}
                        options[task][pars['dest']]['general'] = True

        return options


def process_args(args, kwargs, options):
    """ process arguments"""

    # create the output directory
    if os.path.exists(args.output_dir) and not os.path.isdir(args.output_dir):
        if not ((hasattr(args, 'realtime') and args.realtime)
                or (hasattr(args, 'use_prefit_model') and args.use_prefit_model)):
            dtime = datetime.fromtimestamp(
                os.path.getctime(args.output_dir)).strftime('%y%m%d_%H%M%S')
            name = args.output_dir + '_' + dtime
            os.rename(args.output_dir, name)

    if not os.path.exists(args.output_dir):
        os.makedirs(args.output_dir)

    # create logger
    log_name = os.path.join(args.output_dir, args.task.title() + '_' + args.o_suffix + '.log')
    args.logger = Logger(log_name).logger
    args.date_format = __date_format__

    # standard output the running of module with current Python version
    args.logger.start("""Running task '{task}', using '{prog} v{version}' under Python {py} by '{user}' on
                      '{sys}', output to  '{output}'""".format(
        task=args.task,
        prog=os.path.split(kwargs['__current_module__'])[1],
        version=kwargs['__version__'],
        py='.'.join(map(str, __sys_version__[:3])),
        user=__user__,
        sys=__sys_name__,
        output=args.output_dir)
    )

    if hasattr(args, 'force_cpu') and args.force_cpu:
        # force tensorflow to use CPU only on machines with a GPU
        os.environ["CUDA_VISIBLE_DEVICES"] = ""

    # establish the input database connection if required
    if hasattr(args, 'idb_connstr') and args.idb_connstr not in ['']:

        # TODO: Replace DB connection with DataConnector
        raise Exception("idb_connstr not ready yet")

        # try:
        #     # using turbodbc
        #     args.idb = DataBase(conn_str=args.idb_connstr)
        # except:
        #     raise args.logger.critical(
        #         "failed to connect to the input Database using the connection strings entered with '-d'."
        #     )

    # establish the input database connection if required
    if hasattr(args, 'odb_connstr') and args.odb_connstr != '':
        if args.odb_connstr == "sqlite3":

            try:
                args.odb = create_engine(args.odb_connstr, convert_unicode=True)
            except:
                args.logger.critical("failed to connect to the output Database.")

    if not hasattr(args, 'show'):
        setattr(args, 'show', False)

    if not args.show:
        os.environ["TF_CPP_MIN_LOG_LEVEL"] = "2"  # suppress warning and debug info

    # process list-type options
    for key, option in options.items():
        if option['nargs'] == '+' and type(getattr(args, key)) != list:
            setattr(args, key, [getattr(args, key)])
