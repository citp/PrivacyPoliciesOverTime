import atexit
import sys

PROGRAM_NAME = "python %s" % " ".join(sys.argv) 


try:
    from slack import WebClient
except ImportError:
    pass

sc = None
def init_slack():
    global sc
    if sc is None:
        token = open(os.path.expanduser("~/.slacktoken.txt")).read().strip()
        #print(token)
        sc = WebClient(token)
    return sc

def slack_message(message, channel):
    try:
        WebClient
    except NameError:
        #print("Cannot send message")
        return
    sc = init_slack()
    sc.chat_postMessage(channel=channel, text=message)

def enable_slack_message():
    atexit_hook = lambda: slack_message("COMPLETED: %s" % PROGRAM_NAME, "#programs")
    atexit.register(atexit_hook)
    old_excepthook = sys.excepthook
    def new_excepthook(t, value, tb):
        atexit.unregister(atexit_hook)
        old_excepthook(t,value,tb)

        if t is KeyboardInterrupt:
            return
        
        sio = io.StringIO()
        traceback.print_exception(t,value,tb,100,sio)
        tbs = sio.getvalue()
        sio.close()
        slack_message("FAILED: %s\n%s" % (PROGRAM_NAME, tbs), "#programs")
    sys.excepthook = new_excepthook

