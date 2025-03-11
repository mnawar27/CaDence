import streamlit as st
import pandas as pd
import altair as alt
import plotly.graph_objects as go
import plotly.express as px
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import wordcloud
from wordcloud import WordCloud

import numpy as np

plt.style.use('dark_background')
################################################### PAGE CONFIG
st.set_page_config(
    page_title="CaDence",
    page_icon= "🎶",
    layout="wide",
    initial_sidebar_state="expanded")

alt.themes.enable("dark")
with open('style.css') as f:
    st.markdown(f'<style>{f.read()}</style>', unsafe_allow_html=True)

################ Begin DF corelation to feed into Dashboard
    df_seled_wk =pd.DataFrame(columns=['artist', 'song','duration','ts','sessionId','level','state','userAgent','userId','firstName','gender','week','time_zone'])
#### Have 'present as default
    if sb_wk==[]:
        sb_wk=present
    else:
        sb_wk=sb_wk
#### If no tz, then all tz to avoid errors before selections are made.
    if sb_tz==[]:
        sb_tz=allPlaces
###### If selections are made, go with the selections
    else:
        sb_tz=sb_tz
#### Now that there's a df with the right tz, narrow down to the ones with the right weeks we want    
    for i in sb_tz:
        select_tz= omega_raw.loc[omega_raw['time_zone'] == i]
        df_seled_wk=pd.concat([df_seled_wk,select_tz])
    df_selected_week= df_seled_wk[df_seled_wk['week'].isin(sb_wk)]

###################################################
# Plots
################################################### PAID LEVEL
    def paid_level(df_selected_week):
        paidlev=df_selected_week.drop_duplicates(subset=['userId'])
        paidlev=paidlev.groupby('week')['level'].value_counts()
        paidlev=paidlev.reset_index()
        paidlev.columns = ['Week', 'Level', 'Count']
  #######
        height=paidlev['Count']
        plt.figure(facecolor="black")
        b_colors={'paid':'#00d732','free':'#0073d7'}
        colors=[b_colors[i] for i in paidlev['Level']]
        plt.bar (paidlev['Week'],height,color=colors,width=0.5)
        ax = plt.gca()  
        ax.set_facecolor("black")
        ax.spines['bottom'].set_color('#fbfbfb80')
        ax.spines['left'].set_color('#fbfbfb80')
        ax.xaxis.label.set_color('#fbfbfb80')
        ax.tick_params(axis='x', colors='white')
        ax.tick_params(axis='y', colors='white')
        ax.set_title("Paid vs Free Accounts", fontsize=20, pad=20,color='white')
        pink_patch = mpatches.Patch(color='#00d732', label='Paid')
        blue_patch = mpatches.Patch(color='#0073d7', label='Free')
        plt.legend(handles=[blue_patch,pink_patch])  
        return plt

################################################### DEVICES 

def most_used_platform(df_selected_week):
    platform=df_selected_week.groupby('userId')['userAgent'].first().value_counts()
    # label_tz = ", ".join(df_selected_week['time_zone'].unique())
    if sb_tz==allPlaces:
        label_tz="All Time Zones"
    else:
        label_tz=sb_tz
    ##Donut Chart
    fig = px.pie(platform, names=platform.index,values=platform, hole=.6,color='count',
                color_discrete_map={'Windows':'#dc14c5',
                                    'Mac':'#d84f9e',
                                    'Linux':'#7f43ac',
                                    'iPhone':'#3c34bc',
                                    'iPad':'#1eb75c'})
    fig.update_traces(textinfo='percent+label')
    fig.update_layout(showlegend=False, font_size=15, height=380,width=100,title=dict(text=f"Devices Used in {label_tz}",font=dict(size=20)))
    return fig

################################################### USER COUNT MAP

def get_state_count(omega_raw, time_zone='All', week='All'):
    df = omega_raw.copy()
    if time_zone != 'All': df = df[df['time_zone'] == time_zone]
    if week != 'All': df = df[df['week'] == week]

    groupby_cols = ['state', 'time_zone'] if week == 'All' else ['state', 'time_zone', 'week']
    state_count = df.groupby(groupby_cols)['userId'].nunique().reset_index()

    state_count.columns = ['State', 'Time Zone', 'User Count'] if week == 'All' else ['State', 'Time Zone', 'Week', 'User Count']
    fig = px.choropleth(state_count, locations='State', locationmode='USA-states', color='User Count',
                        hover_name='State', color_continuous_scale="plasma")
    fig.update_layout(mapbox_zoom=9,
    title={
        'text': "Users Per State",
        'x': 0.5,  
        'xanchor': 'center',  
        'font_size':25
    })
    fig.update_geos(visible=True, showcoastlines=True, coastlinecolor="Black", showland=True, landcolor="lightgray", projection_type="albers usa",bgcolor='black')
    return fig

################################################### GENDER

def get_gender(df_selected_week):
    ###### Step one: drop dups since we only want to count users once
    gender_df = df_selected_week.drop_duplicates(subset=['userId'])
    ###### Step two: Now that userId is taken care of, we can agg between just week and gender
    gender_df = gender_df.groupby('week')['gender'].value_counts()
    gender_df=gender_df.reset_index()
    ###### Chart starts now
    height=gender_df['count']
    plt.figure(facecolor="black")
    b_colors={'M':'#0035a7','F':'#fb449a','Other':'#fdbd0c'}
    colors=[b_colors[i] for i in gender_df['gender']]
    plt.bar (gender_df['week'],height,color=colors,width=0.5)
    if sb_tz==allPlaces:
        t_tz="all Time Zones"
    else:t_tz=sb_tz
    plt.title(f"Gender Counts in {t_tz}")
    # plt.figure(figsize=(4,6)) 
    ax = plt.gca()  
    ax.set_facecolor("black")
    ax.spines['bottom'].set_color('#fbfbfb80')
    ax.spines['left'].set_color('#fbfbfb80')
    ax.xaxis.label.set_color('#fbfbfb80')
    ax.tick_params(axis='x', colors='white')
    ax.tick_params(axis='y', colors='white')
    pink_patch = mpatches.Patch(color='#fb449a', label='Female')
    blue_patch = mpatches.Patch(color='#0035a7', label='Male')
    yellow_patch = mpatches.Patch(color='#fdbd0c', label='Other')
    plt.legend(handles=[blue_patch,pink_patch,yellow_patch])
    return plt