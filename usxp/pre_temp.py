
class ProcessingObject(object):

    def __init__(self, res, years):
        self.res = res
        self.years = years
        self.traj_dataset = "traj_cdl"+str(self.res)+"_b_"+str(self.years)

    def __str__(self):
        return 'ProcessingObject(res: {}, years: {})'.format(self.res, str(self.years))



pre = None


def run():
    print "pre is: {}".format(str(pre))

    print 'dsdsdsd', pre.traj_dataset 


