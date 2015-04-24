# coding=utf8

# Get list of local logical devices (mounts on linux, drives on windows)
# RETURNS: dictionary  [mount_point] = volume_name or mounted from
#
# NOTE: on windows network drives are ignored
#
# 	TOCHECK:Does this works on XP?
# 	TODO: make for unix/linux
def get_drives2():
    if sys.platform =='win32':
        result = os.popen("mountvol").read()
        idx = result.find(' \\')
        if idx<0:
            return []
        out = filter(len, result[idx:].splitlines() )
        drives = {}
        for i in xrange(0, len(out),2):
            disk = out[i+1].strip()
            if disk.find(":\\")>0:
                drives[ disk ] = out[i].strip()
        return drives
    elif sys.platform=='posix':
        return []

