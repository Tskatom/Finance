# -*- coding:utf8-*-
import re
content = """Good Argentina’s central bank has temporarily suspended a change to part of its charter that would require banks to hold reserves at the central bank, Ambito Financiero reported.
Argentine banks can maintain the amount of savings needed to meet their reserve requirements in their own accounts as previously allowed, the Buenos Aires-based newspaper reported, citing a statement from the central bank.
The move will help sustain liquidity in the banking system and keep interest rates lower, Ambito Financiero reported."""

print content
rule = "(Ambito Financiero|argentina)"
p = re.compile(rule,re.I)
m = p.findall(content)
if m:
    print "Matched: ", m
    m = {}.fromkeys(m).keys()
    print m
else:
    print "No Match"
    
print __file__

import os
print os.path.dirname(os.path.dirname(__file__))
print os.path.basename(__file__)
print os.path.join(os.path.dirname(__file__),"test.conf")