import boto3
import matplotlib.pyplot as plt
from matplotlib.lines import Line2D
import matplotlib.dates as mdates
from matplotlib.ticker import FormatStrFormatter, LinearLocator

# Matplotlib config
plt.style.use('ggplot')
plt.rcParams['font.family'] = 'serif'
plt.rcParams['font.serif'] = 'Ubuntu'
plt.rcParams['font.monospace'] = 'Ubuntu Mono'
plt.rcParams['font.size'] = 12
plt.rcParams['axes.labelsize'] = 12
plt.rcParams['axes.labelweight'] = 'bold'
plt.rcParams['xtick.labelsize'] = 12
plt.rcParams['ytick.labelsize'] = 12
plt.rcParams['legend.fontsize'] = 12
plt.rcParams['figure.titlesize'] = 14
colors = [pc['color'] for pc in plt.rcParams['axes.prop_cycle']]

# Client setup --> US East 2 around 2x cheaper than US East 1!
client1 = boto3.client('ec2', region_name='us-east-1')
client2 = boto3.client('ec2', region_name='us-east-2')

# Specify instance types and subregions
instance_types = ['c5.xlarge']
subregions1 = ['us-east-1a', 'us-east-1b', 'us-east-1c', 'us-east-1d', 'us-east-1e', 'us-east-1f']
subregions2 = ['us-east-2a', 'us-east-2b', 'us-east-2c']

# Get prices for certain instance type and OS
prices1 = client1.describe_spot_price_history(InstanceTypes=instance_types,
	                                          ProductDescriptions=['Linux/UNIX'])['SpotPriceHistory']
prices2 = client2.describe_spot_price_history(InstanceTypes=instance_types,
	                                          ProductDescriptions=['Linux/UNIX'])['SpotPriceHistory']

# Read into lists that can be plotted
prices_clean1 = {it: {sr: [] for sr in subregions1} for it in instance_types}
prices_clean2 = {it: {sr: [] for sr in subregions2} for it in instance_types}

for p in prices1:
	prices_clean1[p['InstanceType']][p['AvailabilityZone']].append((p['Timestamp'], float(p['SpotPrice'])))
for p in prices2:
	prices_clean2[p['InstanceType']][p['AvailabilityZone']].append((p['Timestamp'], float(p['SpotPrice'])))

# Merge dicts
prices_clean = {}
for it in instance_types:
	dummy = {it: {**prices_clean1[it], **prices_clean2[it]}}
	prices_clean.update(dummy)

# Create figure and axis
fig, axes = plt.subplots(len(instance_types), 1, figsize=(8, 4 * len(instance_types)))

# Silly hack
axes = [axes]

# To get the weekly ticks right
weeks = mdates.WeekdayLocator()
weeks_format = mdates.DateFormatter('%Y-%m-%d')

# Plot by looping through dict
for it, ax in zip(prices_clean.items(), axes):
	for sr, p in it[1].items():
		if sr[-2] == '1':
			ax.plot(*zip(*p), color=colors[0], label=sr)
		else:
			ax.plot(*zip(*p), color=colors[1], label=sr)

	ax.set_title(f'Differences between regions: {it[0]}', fontstyle='italic')
	# ax.set_xlabel('date [-]')
	ax.set_ylabel('price [$/hr]')
	ax.xaxis.set_major_locator(weeks)
	ax.xaxis.set_major_formatter(weeks_format)
	# ax.yaxis.set_major_locator(LinearLocator(5))
	# ax.yaxis.set_major_formatter(FormatStrFormatter('%.3f'))
	legend = [Line2D([0], [0], color=colors[0], lw=1.5, label='us-east-1(a-f)'),
		  	  Line2D([0], [0], color=colors[1], lw=1.5, label='us-east-2(a-c)')]
	ax.legend(handles=legend)

fig.autofmt_xdate()
fig.tight_layout()
fig.savefig('../figures/regiondiff.pdf')
plt.show()
