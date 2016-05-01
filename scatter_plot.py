import pandas as pd

from bokeh.plotting import Figure, show, output_file, ColumnDataSource

df = pd.read_csv('success_class_housing.csv')
print df.head()

df_success = df[df['success_class']==1]
df_fail = df[df['success_class'] == 0]

source1 = ColumnDataSource(data=df_success)
source2 = ColumnDataSource(data=df_fail)

p = Figure(title='Success Metric vs Housing Costs',
           x_axis_label='$/sq. ft.',
           y_axis_label='success_metric',
           y_range=(0,5),
           tools=['crosshair,resize,reset,save'])

p.circle('2016_02', 'success_metric', color='green',alpha=0.3, source=source1)
p.circle('2016_02', 'success_metric', color='red',alpha=0.3, source=source2)


output_file("success_class_housing.html", title="Success vs Housing cost")
show(p)
