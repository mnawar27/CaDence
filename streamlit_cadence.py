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