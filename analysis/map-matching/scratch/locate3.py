# -*- coding: utf-8 -*-
"""
Created on Fri Oct  1 07:12:44 2021

@author: WylieTimmerman
"""
r_df.edges
r_df.edges.explode()
list(map(lambda x: x.get('edge_info').get('shape'),r_df.edges.explode()))

    
test = r_df.iloc[27,:]



r_df3 = (
    r_df
    .assign(
        shape = lambda x: 
            list(
                map(
                    lambda y: y.get('edge_info').get('shape'),
                    x.edges.explode()
                )
            ),
        edge_id = lambda x: 
            list(
                map(
                    lambda y: y.get('edge_id').get('value'),
                    x.edges.explode()
                )
            )
    )
    .drop(['edges','nodes'], axis = "columns")
)
