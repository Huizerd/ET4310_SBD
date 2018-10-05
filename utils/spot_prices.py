import boto3
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

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

# Client setup --> US East 2 around 2x cheaper than US East 1!
client = boto3.client('ec2', region_name='us-east-2')

# Specify instance types and subregions
instance_types = ['c4.8xlarge', 'c5.9xlarge']
subregions = ['us-east-2a', 'us-east-2b', 'us-east-2c']

# Get prices for certain instance type and OS
prices = client.describe_spot_price_history(InstanceTypes=instance_types,
	                                        ProductDescriptions=['Linux/UNIX'])['SpotPriceHistory']

# Read into lists that can be plotted
prices_clean = {it: {sr: [] for sr in subregions}  for it in instance_types}

for p in prices:
	prices_clean[p['InstanceType']][p['AvailabilityZone']].append((p['Timestamp'], float(p['SpotPrice'])))

# Create figure and axis
fig, axes = plt.subplots(len(instance_types), 1, figsize=(8, 4 * len(instance_types)))

# To get the weekly ticks right
weeks = mdates.WeekdayLocator()
weeks_format = mdates.DateFormatter('%Y-%m-%d')

# Plot by looping through dict
for it, ax in zip(prices_clean.items(), axes.flat):
	for sr, p in it[1].items():
		ax.plot(*zip(*p), label=sr)

	ax.set_title(f'Spot instance prices: {it[0]}', fontstyle='italic')
	ax.set_xlabel('date [-]')
	ax.set_ylabel('price [$/hr]')
	ax.xaxis.set_major_locator(weeks)
	ax.xaxis.set_major_formatter(weeks_format)
	ax.legend()

fig.autofmt_xdate()
fig.tight_layout()
plt.show()
