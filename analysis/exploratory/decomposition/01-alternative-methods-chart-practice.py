# -*- coding: utf-8 -*-
"""
Created on Tue Jun  8 06:10:12 2021

@author: WylieTimmerman
"""

# plotly basics
df = px.data.gapminder().query("continent != 'Asia'") # remove Asia for visibility
fig = px.line(df, x="year", y="lifeExp", color="continent",
              line_group="country", hover_name="country")
plot(fig)


# test case
filetest = "rawnav02626191023.txt"
index_test = 6680

test_fil11 = (
    rawnav_fil11
    .query('filename == @filetest & index_run_start == @index_test')
)

fig2 = (
    px.line(
        test_fil11,
        x = 'sec_past_st',
        y = 'odom_ft'       
    )
)
    
plot(fig2)

fig2 = (
    px.line(
        test_fil11,
        x = 'sec_past_st',
        y = 'odom_ft',
        color = 'high_level_decomp',
        line_group = 'sequence' # need to make unique id, but for now just showing one
    )
)
    
plot(fig2)
