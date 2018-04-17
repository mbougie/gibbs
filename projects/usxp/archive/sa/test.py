import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker 

data = pd.DataFrame.from_csv('C:\\Users\\Bougie\\Desktop\\Gibbs\\sa\\compare2fsa.csv', sep=',')

print data['fips']


categories = data.fips.unique()
mydata = {}
for i in categories:
    mydata[i] = data[data.fips==i]

mydata[categories[0]].head()

nrows = 41; ncols = 3
num_plots = nrows * ncols  # number of subplots
print num_plots

print mydata
print len(mydata)
# assert num_plots == 41

figwidth = 13/1.1
figheight = 10/1.2

x_range = range(2004,2015,1)

fig = plt.figure(figsize=(figwidth, figheight))
#fig = plt.figure()
# create the subplot figure
axes = [plt.subplot(nrows,ncols,i) for i in range(1,num_plots+1)]

# h_pad is horizontal padding between subplots
# w_pad is vertical padding between subplots
plt.tight_layout(pad=0, w_pad=3, h_pad=3)
#plt.subplots_adjust(left=None, bottom=None, right=None, top=None, wspace=None, hspace=0.01)
plt.subplots_adjust(hspace=1)
plt.rcParams['xtick.major.pad']='8'

print 'len(categories)', len(categories)

for i in range(len(categories)):
    ax = axes[i]
    y = data["acres"][data.category==categories[i]]
    x = x_range
    ax.plot(x,y,color='#222222')
    ax.fill_between(x,y,0,color='#cec6b9')

    #ax.xticks(['2004','2014'])
    ax.set_ylim([0,3000])
    ax.set_xlim([2004,2014])
    
    # Remove top and right axes and ticks
    ax.spines['top'].set_visible(False)
    ax.spines['bottom'].set_visible(False)
    ax.spines['left'].set_visible(False)
    ax.spines['right'].set_visible(False)

    ax.yaxis.set_ticks_position('none')
    ax.xaxis.set_ticks_position('none')
    
    #ax.xticks([0,1], ['2004','2014'], rotation='horizontal')    
    # Find at most 101 ticks on the y-axis at 'nice' locations
    max_yticks = 3
    yloc = plt.MaxNLocator(max_yticks)
    ax.yaxis.set_major_locator(yloc)

    #max_xticks = 4
    #xloc = plt.MaxNLocator(max_xticks)
    #ax.xaxis.set_major_locator(xloc)
    ax.set_xticklabels(['2004','','','','','2014'])
    
    ax.set_xlabel(categories[i])
    ax.tick_params(axis='both', which='major', labelsize=8)
    
    def func(x, pos):  # formatter function takes tick label and tick position
       s = '{:0,d}'.format(int(x))
       return s

    ax.yaxis.set_major_formatter(mticker.FuncFormatter(func)) 

plt.show()