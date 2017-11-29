import os


def filtermails():
    for filename in os.listdir("./mails/"):
        print("Hi")
        if 'Subject' not in open("./mails/" + filename, encoding='latin-1').read():
            os.remove("./mails/" + filename)
            print(filename)


filtermails()
