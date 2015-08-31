#!/usr/bin/python

import rpm, os, sys, glob, getopt

def usage():
	print os.path.basename(sys.argv[0]),"[-h] [-d] [-t] -r <rpm repository path>"
	print """
	Removes older versions of rpm from repository
	-h,--help \t\t\t\t print this help 
	-v,--verbose \t\t\t\t verbose output
	-d,--dry-run \t\t\t\t do not perform any actions -  dry run
	-n,--keep-number \t\t\t keep the number of packages
	-r,--repopath \t\t\t\t path to repository
	"""

try:
	opts, args = getopt.getopt(sys.argv[1:], "hdtr:vn:", ["help", "debug", "test",  "repopath="])
except getopt.GetoptError, err:
	print str(err) # will print something like "option -a not recognized"
	usage()
	sys.exit(2)

debug = False
dryrun = False
keep_num = 1
for o, a in opts:
	if o   in ("-v", "--verbose"): debug = True
	elif o in ("-d", "--dry-run"): dryrun = True
	elif o in ("-h", "--help"):
		usage()
		sys.exit()
	elif o in ("-n", "--keep-number"):
		if o >= 1:
			keep_num = int(a)
		else:
			assert False, "Number of keep packages must be >=1" 
	elif o in ("-r", "--repopath"):	repopath = a
	else:	assert False, "unhandled option"

try:
	repopath
except NameError:
	usage()	
	sys.exit(2)

ts = rpm.TransactionSet()
ts.setVSFlags(rpm._RPMVSF_NOSIGNATURES)

pkgnames = {}
hdr_name_map = {} 

if repopath[-1] == '/' : repopath = repopath[:-1]
pkglist = glob.glob(repopath+'/*.rpm')

for p in pkglist:
	fdno = os.open(p, os.O_RDONLY)
	hdr = ts.hdrFromFdno(fdno)
	os.close(fdno)
	thename	= hdr[rpm.RPMTAG_NAME] + '.' + hdr[rpm.RPMTAG_ARCH]

	hdr_name_map[hdr] = p
	if not pkgnames.has_key(thename):
		pkgnames[thename] = []
	pkgnames[thename].append(hdr)

for i in sorted(pkgnames.keys()):
	pkgnames[i] = sorted(pkgnames[i], rpm.versionCompare)
	if debug: print i, len(pkgnames[i])		
	if len(pkgnames[i]) > 1:
		for _n in range(1, keep_num+1):
			keep = pkgnames[i].pop()
			if debug: print '==keep==', hdr_name_map[keep]
		for v in pkgnames[i]:
			thepackage = hdr_name_map[v]
			if debug: print '==removing==', thepackage
			if not dryrun: os.remove(thepackage)
