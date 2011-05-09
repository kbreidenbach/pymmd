import pymmd

class Echo3(pymmd.MMDService):
    def set_channel(self, ch):
        self.ch = ch

    def handle_call(self, msg):
        print "handle_call(%s)" % repr(msg)
        return ["pong", msg.body]

    def handle_subscribe(self, msg):
        print "handle_subscribe(%s)" % repr(msg)

    def handle_message(self, msg):
        print "handle_message(%s)" % repr(msg)
        if type(msg.body) != list and msg.body[0] != "pong2":
            msg.reply(["pong2", msg.body])

    def handle_close(self, msg):
        print "handle_close(%s)" % repr(msg)

echo3 = Echo3()
