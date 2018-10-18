import boto3
import json
import matplotlib.pyplot as plt
from matplotlib.lines import Line2D
import datetime
import re

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
facecolor = plt.rcParams['axes.facecolor']

# Client setup for pricing (instance attributes) and EC2 (spot pricing)
pricing_client = boto3.client('pricing', region_name='us-east-1')
ec2_client = boto3.client('ec2', region_name='us-east-2')

# What instances to check, and which instances to put on the x and y-axis
# Cheapest subregion is usually us-east-2c
instance_types = ['c4.large', 'c4.xlarge', 'c4.8xlarge', 'c5.large', 'c5.xlarge', 'm4.large', 'm5.large',
				  'm4.xlarge', 'm5.xlarge', 'r5.xlarge', 'r5.large', 'r4.large', 'r4.xlarge']
attributes = ['memory', 'ecu']
subregions = ['us-east-2a', 'us-east-2b', 'us-east-2c']

### Get instance attributes ###

# Product filters
service_code = 'AmazonEC2'
filters = [[{'Type': 'TERM_MATCH', 'Field': 'instanceType', 'Value': it}] for it in instance_types]

# Build product dict
products = {it: {att: json.loads(pricing_client.get_products(ServiceCode=service_code, Filters=filt, MaxResults=1)['PriceList'][0])['product']['attributes'][att] for att in attributes} for it, filt in zip(instance_types, filters)}

# Convert to integers
for it, att in products.items():
	for a, v in att.items():
		products[it][a] = float(re.search(r'\d*\.*\d*', v).group())

### Get latest spot price ###

# Get prices (only latest) for certain instance type and OS
prices = ec2_client.describe_spot_price_history(InstanceTypes=instance_types,
	                                            ProductDescriptions=['Linux/UNIX'],
	                                            StartTime=datetime.datetime.utcnow())['SpotPriceHistory']

# Read into lists that can be plotted
prices_clean = {it: {sr: 0 for sr in subregions}  for it in instance_types}

for p in prices:
	prices_clean[p['InstanceType']][p['AvailabilityZone']] = float(p['SpotPrice'])

# Create figure and axis
fig, ax = plt.subplots(figsize=(8, 4))

# Custom offsets for annotations
y_offsets = [-8, 0, 0, 0, 0, -10, -10, 5, 5, 0, 0, 0, 0]

def my_round(x, prec=3, base=.001):
  return round(base * round(float(x)/base), prec)

# Plot by looping through dict
for i, (it, att) in enumerate(products.items()):
	if it[0] == 'c':
		color = colors[0]
	elif it[0] == 'm':
		color = colors[1]
	elif it[0] == 'r':
		color = colors[2]

	best_subregion = min(prices_clean[it], key=prices_clean[it].get)
	min_price = prices_clean[it][best_subregion]

	ax.scatter(att[attributes[0]] / min_price, att[attributes[1]] / min_price, color=color)
	ax.annotate(it, (att[attributes[0]] / min_price, att[attributes[1]] / min_price + y_offsets[i]))

ax.set_title(f'Cost efficiency of AWS EC2 instances in us-east-2', fontstyle='italic')
ax.set_xlabel('memory efficiency [GB/($/hr)]')
ax.set_ylabel('ECU efficiency [-/($/hr)]')

legend = [Line2D([0], [0], marker='o', color=facecolor, label='compute optimized',
                          markerfacecolor=colors[0], markersize=10),
		  Line2D([0], [0], marker='o', color=facecolor, label='general purpose',
                          markerfacecolor=colors[1], markersize=10),
		  Line2D([0], [0], marker='o', color=facecolor, label='memory optimized',
                          markerfacecolor=colors[2], markersize=10)]
ax.legend(handles=legend)

fig.tight_layout()
fig.savefig('../figures/costefficiency.pdf')
plt.show()
