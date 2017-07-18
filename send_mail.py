SENDMAIL = "/usr/sbin/sendmail" # sendmail location

FROM = "adsnsrg@gmail.com"
TO = ["adsnsrg@gmail.com"]

SUBJECT = "Hello!"

TEXT = "This message was sent via sendmail."

# Prepare actual message

message = """\
From: %s
To: %s
Subject: %s

%s
""" % (FROM, ", ".join(TO), SUBJECT, TEXT)

# Send the mail

import os

p = os.popen("%s -t -i" % SENDMAIL, "w")
p.write(message)
status = p.close()
if status:
    print ("Sendmail exit status", status)