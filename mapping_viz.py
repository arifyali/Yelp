#from pyspark.sql import HiveContext

import pandas as pd
import numpy as np

from bokeh.plotting import figure, show, output_file
from bokeh.sampledata import us_states, us_counties

#sc = SparkContext()
#sqlContext = HiveContext(sc)
#qry = """SELECT success_metric, zipcode FROM census_rest_success"""

#df = sqlContext.sql(qry).toPandas()
df = pd.read_csv('success_by_zip.csv')
df_county = pd.read_excel('ZIP_COUNTY_032016.xlsx')
df_county['zipcode'] = df_county['ZIP']

data = df.merge(df_county, on='zipcode', how='inner')
data = data[['success_metric', 'COUNTY']].groupby('COUNTY').mean().reset_index()

us_states = us_states.data.copy()
us_counties = us_counties.data.copy()

del us_states["HI"]
del us_states["AK"]

state_xs = [us_states[code]["lons"] for code in us_states]
state_ys = [us_states[code]["lats"] for code in us_states]

county_xs=[us_counties[code]["lons"] for code in us_counties if us_counties[code]["state"] not in ["ak", "hi", "pr", "gu", "vi", "mp", "as"]]
county_ys=[us_counties[code]["lats"] for code in us_counties if us_counties[code]["state"] not in ["ak", "hi", "pr", "gu", "vi", "mp", "as"]]

colors = ["#F1EEF6", "#D4B9DA", "#C994C7", "#DF65B0", "#DD1C77", "#980043"]

counties = {
    code: county for code, county in us_counties.items()
}

data = data.to_dict()['success_metric']
print type(data.keys()[0])
print counties


county_colors = []
for county_id in counties:
    try:
      metric = data[county_id]
      norm_metric = (metric - min(data.values()))/(max(data.values()) - min(data.values()))
      idx = min(int(7*norm_metric), 5)
      county_colors.append(colors[idx])
    except KeyError:
      county_colors.append("black")

output_file("success_by_county.html", title="Success By County")
p = figure(title="Success By County", toolbar_location="left", plot_width=1100, plot_height=700)
p.patches(county_xs, county_ys, fill_color=county_colors, fill_alpha=0.9, line_color="white", line_width=0.5)
p.patches(state_xs, state_ys, fill_alpha=0.2, line_color="#884444", line_width=2)

show(p)
