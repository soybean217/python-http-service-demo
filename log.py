import logging
import sys

# logger = logging.getLogger('fm')
# logger.setLevel(logging.DEBUG)

# ch = logging.StreamHandler(stream=sys.stdout)
# ch.setLevel(logging.DEBUG)

# formatter = logging.Formatter(
#     '%(asctime)s - %(levelname)s %(filename)s:%(lineno)s %(funcName)s - %(message)s')
# # fh.setFormatter(formatter)
# ch.setFormatter(formatter)

# # logger.addHandler(fh)
# logger.addHandler(ch)

handler = logging.StreamHandler(stream=sys.stdout)
fmt = '%(asctime)s - %(levelname)s %(filename)s:%(lineno)s - %(funcName)s - %(message)s'

formatter = logging.Formatter(fmt)
handler.setFormatter(formatter)

logger = logging.getLogger('fm')
logger.addHandler(handler)
logger.setLevel(logging.DEBUG)

# def my_handler(type, value, tb):
#     print('HELOOOEIJOFJOSEJF')
#     logger.error("Uncaught exception: {0}".format(str(value)))
#
# # Install exception handler
#
# sys.excepthook = my_handler
