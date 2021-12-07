#!/usr/bin/env python3

import os
from zipfile import ZipFile

folders = os.listdir()

for f in folders:
	if not (os.path.isfile(f) or f[0] == '.'):
		thisDir = os.path.join(f)
		# print(os.path.join(thisDir, f + '.zip'))
		files = os.listdir(thisDir)
		# print(files)
		zipPath = os.path.join(thisDir, f + '.zip')
		if os.path.exists(zipPath):
			os.remove(zipPath)

		zipObj = ZipFile(zipPath, 'w')

		for file in files:
			if file.endswith('.txt'):
				print(os.path.join(thisDir, file))
				zipObj.write(os.path.join(thisDir, file), file)
		
		zipObj.close()
		print('Done with ' + zipPath)