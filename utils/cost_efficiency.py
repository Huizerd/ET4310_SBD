import boto3
import json
import matplotlib.pyplot as plt
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

# Client setup for pricing (instance attributes) and EC2 (spot pricing)
pricing_client = boto3.client('pricing', region_name='us-east-1')
ec2_client = boto3.client('ec2', region_name='us-east-2')

# What instances to check, and which instances to put on the x and y-axis
# Cheapest subregion is usually us-east-2c
instance_types = ['c4.large', 'c4.8xlarge', 'c5.large', 'c5.xlarge', 'm5.large']
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

# Plot by looping through dict
for it, att in products.items():
	best_subregion = min(prices_clean[it], key=prices_clean[it].get)
	min_price = prices_clean[it][best_subregion]

	ax.scatter(att[attributes[0]] / min_price, att[attributes[1]] / min_price, color=colors[0])
	ax.annotate(it + ' (' + best_subregion + ')', (att[attributes[0]] / min_price,att[attributes[1]] / min_price))

ax.set_title(f'Cost efficiency of AWS EC2 instances', fontstyle='italic')
ax.set_xlabel('memory efficiency [GB/($/hr)]')
ax.set_ylabel('ECU efficiency [-/($/hr)]')

fig.tight_layout()
plt.show()
