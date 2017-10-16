from lettuce import step
from time import sleep

@step(u'I print "([^"]*)"$')
def i_print(step, text):
    print "\n"
    print text

@step(u'I print "([^"]*)" and wait "([^"]*)"$')
def i_print(step, text, wait):
    sleep(int(wait))
    print "\n"
    print text

@step(u'I fail step$')
def i_fail(step):
    assert False, "Failed step"