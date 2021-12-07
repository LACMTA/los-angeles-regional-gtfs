#!/usr/bin/env python3

import os
import fileinput
import re

def replace_Trillium(text):
	text = text.replace("http://www.trilliumtransit.com", "https://www.metro.net")
	text = text.replace("Trillium Solutions, Inc.", "Los Angeles County Metropolitan Transportation Authority")
	text = text.replace("http://support.trilliumtransit.com", "https://developer.metro.net")
	text = re.sub(r'support.*@trilliumtransit\.com', 'csinteractive@metro.net', text)
	return text

thisdir = os.getcwd()

for r, d, f in os.walk(thisdir):
	for file in f:
		if file.endswith("feed_info.txt"):
			print(os.path.join(r, file))
			with open(os.path.join(r, file), 'r') as f:
				filedata = f.read()
			filedata = replace_Trillium(filedata)
			with open(os.path.join(r, file), 'w') as f:
				f.write(filedata)
