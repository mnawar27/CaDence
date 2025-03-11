
# import streamlit as st
# import pandas as pd
# import mysql.connector
# from sqlalchemy import create_engine
# import time
# import plotly.graph_objects as go
# import plotly.express as px

# # Establish connection to MySQL database
# engine = create_engine("mysql+pymysql://d52:DinGrogu@xo.zipcode.rocks:3388/data_52")

# # Function to fetch all data from results_table
# def fetch_data():
#     query = "SELECT * FROM results_table"
#     df = pd.read_sql(query, engine)
#     return df

# # Function to plot gender bar chart using Plotly
# def plot_gender_chart(male_count, female_count):
#     fig = go.Figure(data=[
#         go.Bar(x=['Male', 'Female'], y=[male_count, female_count], marker_color=['skyblue', 'lightcoral'])
#     ])
#     fig.update_layout(title='Gender Distribution', yaxis_title='Count')
#     return fig

# # Function to plot level bar chart using Plotly
# def plot_level_chart(paid_count, free_count):
#     fig = go.Figure(data=[
#         go.Bar(x=['Paid', 'Free'], y=[paid_count, free_count], marker_color=['green', 'orange'])
#     ])
#     fig.update_layout(title='User Level Distribution', yaxis_title='Count')
#     return fig

# # Function to plot a US map with time zone counts
# def plot_time_zone_map(row, row_index):
#     # Define the states for each timezone
#     timezones = {
#         'Eastern': ['ME', 'VT', 'NH', 'MA', 'RI', 'CT', 'NY', 'NJ', 'PA', 'DE', 'MD', 'VA', 'NC', 'SC', 'GA', 'FL', 'MI', 'OH', 'IN', 'KY', 'TN', 'WV', 'AL', 'Mississippi', 'Louisiana', 'Arkansas', 'Wisconsin', 'Illinois'],
#         'Central': ['MN', 'IA', 'MO', 'WI', 'ND', 'SD', 'NE', 'KS', 'OK', 'TX', 'MT','MS','LA', 'WY', 'CO', 'New Mexico', 'Arizona', 'Idaho', 'Utah'],
#         'Pacific': ['WA', 'OR', 'CA', 'NV'],
#         'Mountain': ['CO', 'NM', 'WY', 'AZ', 'ID', 'MT', 'UT']
#     }

#     # Define the count for each timezone based on the current row
#     counts = {
#         'Eastern': row['eastern_count'],
#         'Central': row['central_count'],
#         'Pacific': row['pacific_count'],
#         'Mountain': row['mountain_count']
#     }

#     # Sort timezones by their counts in descending order
#     sorted_timezones = sorted(counts.items(), key=lambda x: x[1], reverse=True)
    
#     # Define gradient of blue from dark to light for each timezone
#     colors = {
#         sorted_timezones[0][0]: 'darkblue',   # Highest count - Dark blue
#         sorted_timezones[1][0]: 'mediumblue',  # Second highest - Medium blue
#         sorted_timezones[2][0]: 'lightblue',   # Third highest - Light blue
#         sorted_timezones[3][0]: 'skyblue'      # Lowest count - Sky blue
#     }

#     # Create a base map where all states are light grey initially
#     state_colors = {state: 'lightgrey' for state in timezones['Eastern'] + timezones['Central'] + timezones['Pacific'] + timezones['Mountain']}

#     # Color all states in their corresponding timezone color
#     for timezone, color in colors.items():
#         for state in timezones[timezone]:
#             state_colors[state] = color

#     # Create the plot
#     fig = go.Figure()

#     # Add all states with the correct color
#     for state, color in state_colors.items():
#         fig.add_trace(go.Choropleth(
#             locations=[state],
#             locationmode='USA-states',
#             z=[1],  # We use a constant value to ensure the color applies correctly
#             colorscale=[[0, color], [1, color]],  # Color scale is fixed to the determined color
#             showscale=False
#         ))

#     fig.update_layout(
#         geo=dict(
#             scope='usa',
#             projection_type='albers usa',
#             showland=True,
#             landcolor='lightgray',
#             subunitcolor='gray',
#         ),
#         title=f'Timezone Count Highlighted on US Map (Row {row_index + 1})'
#     )

#     return fig, sorted_timezones[0][0]  # Return the highest timezone name

# # Streamlit app to display the data
# def display_data():
#     st.title("User Data Visualizations")
    
#     # Create side-by-side layout
#     col1, col2 = st.columns(2)

#     # Empty containers for the plots
#     gender_plot_container = col1.empty()
#     level_plot_container = col2.empty()
#     map_plot_container = st.empty()
#     timezone_name_container = st.empty()

#     # Fetch all rows from the results_table
#     df = fetch_data()

#     # Iterate over each row in the dataframe
#     for i, row in df.iterrows():
#         # Extract the data from the current row
#         male_count = row['male_count']
#         female_count = row['female_count']
#         paid_count = row['paid_count']
#         free_count = row['free_count']
        
#         # Generate and update the gender plot
#         gender_fig = plot_gender_chart(male_count, female_count)
#         gender_plot_container.plotly_chart(gender_fig, key=f'gender_chart_{i}')

#         # Generate and update the level plot
#         level_fig = plot_level_chart(paid_count, free_count)
#         level_plot_container.plotly_chart(level_fig, key=f'level_chart_{i}')

#         # Generate and update the time zone map plot
#         map_fig, max_timezone = plot_time_zone_map(row, i)
#         map_plot_container.plotly_chart(map_fig, key=f'map_chart_{i}')
        
#         # Display the timezone with the highest count below the map
#         timezone_name_container.markdown(f"**Timezone with highest count: {max_timezone}**")

#         # Wait for a short time before updating to the next row
#         time.sleep(2)  # Adjust this to the desired update interval

# # Run the Streamlit app
# if __name__ == "__main__":
#     display_data()

import streamlit as st
import pandas as pd
import mysql.connector
from sqlalchemy import create_engine
import time
import plotly.graph_objects as go
from wordcloud import WordCloud

# Establish connection to MySQL database
engine = create_engine("mysql+pymysql://d52:DinGrogu@xo.zipcode.rocks:3388/data_52")

# Function to fetch all data from results_table
def fetch_data():
    query = "SELECT * FROM results_table"
    df = pd.read_sql(query, engine)
    return df

# Function to plot gender bar chart using Plotly
def plot_gender_chart(male_count, female_count):
    fig = go.Figure(data=[
        go.Bar(x=['Male', 'Female'], y=[male_count, female_count], marker_color=['skyblue', 'lightcoral'])
    ])
    fig.update_layout(title='Gender Distribution', yaxis_title='Count')
    return fig

# Function to plot level bar chart using Plotly
def plot_level_chart(paid_count, free_count):
    fig = go.Figure(data=[
        go.Bar(x=['Paid', 'Free'], y=[paid_count, free_count], marker_color=['green', 'orange'])
    ])
    fig.update_layout(title='User Level Distribution', yaxis_title='Count')
    return fig

# Function to plot a US map with time zone counts
def plot_time_zone_map(row, row_index):
    # Define the states for each timezone
    timezones = {
        'Eastern': ['ME', 'VT', 'NH', 'MA', 'RI', 'CT', 'NY', 'NJ', 'PA', 'DE', 'MD', 'VA', 'NC', 'SC', 'GA', 'FL', 'MI', 'OH', 'IN', 'KY', 'TN', 'WV', 'AL', 'MS', 'LA', 'AR', 'WI', 'IL'],
        'Central': ['MN', 'IA', 'MO', 'WI', 'ND', 'SD', 'NE', 'KS', 'OK', 'TX', 'MT', 'MS', 'LA', 'WY', 'CO', 'NM', 'AZ', 'ID', 'UT'],
        'Pacific': ['WA', 'OR', 'CA', 'NV'],
        'Mountain': ['CO', 'NM', 'WY', 'AZ', 'ID', 'MT', 'UT']
    }

    # Define the count for each timezone based on the current row
    counts = {
        'Eastern': row['eastern_count'],
        'Central': row['central_count'],
        'Pacific': row['pacific_count'],
        'Mountain': row['mountain_count']
    }

    # Sort timezones by their counts in descending order
    sorted_timezones = sorted(counts.items(), key=lambda x: x[1], reverse=True)

    # Define gradient of blue from dark to light for each timezone
    colors = {
        sorted_timezones[0][0]: 'darkblue',  # Highest count - Dark blue
        sorted_timezones[1][0]: 'mediumblue',  # Second highest - Medium blue
        sorted_timezones[2][0]: 'lightblue',  # Third highest - Light blue
        sorted_timezones[3][0]: 'skyblue'  # Lowest count - Sky blue
    }

    # Create a base map where all states are light grey initially
    state_colors = {state: 'lightgrey' for state in timezones['Eastern'] + timezones['Central'] + timezones['Pacific'] + timezones['Mountain']}

    # Color all states in their corresponding timezone color
    for timezone, color in colors.items():
        for state in timezones[timezone]:
            state_colors[state] = color

    # Create the plot
    fig = go.Figure()

    # Add all states with the correct color
    for state, color in state_colors.items():
        fig.add_trace(go.Choropleth(
            locations=[state],
            locationmode='USA-states',
            z=[1],  # We use a constant value to ensure the color applies correctly
            colorscale=[[0, color], [1, color]],  # Color scale is fixed to the determined color
            showscale=False
        ))

    fig.update_layout(
        geo=dict(
            scope='usa',
            projection_type='albers usa',
            showland=True,
            landcolor='lightgray',
            subunitcolor='gray',
        ),
        title=f'Timezone Count Highlighted on US Map (Row {row_index + 1})'
    )

    return fig, sorted_timezones[0][0]  # Return the highest timezone name

# Function to generate a WordCloud from top songs
def generate_wordcloud(text):
    wordcloud = WordCloud(width=800, height=400, background_color='white').generate(text)
    return wordcloud

# Streamlit app to display the data
def display_data():
    st.title("User Data Visualizations")

    # Create side-by-side layout
    col1, col2 = st.columns(2)

    # Empty containers for the plots
    gender_plot_container = col1.empty()
    level_plot_container = col2.empty()
    map_plot_container = st.empty()
    timezone_name_container = st.empty()
    wordcloud_container = st.empty()

    # Fetch all rows from the results_table
    df = fetch_data()

    # Collect all top songs for WordCloud
    all_top_songs = " ".join(df['top_song'].dropna())

    # Iterate over each row in the dataframe
    for i, row in df.iterrows():
        # Extract the data from the current row
        male_count = row['male_count']
        female_count = row['female_count']
        paid_count = row['paid_count']
        free_count = row['free_count']
        top_song = row['top_song']  # Current row's top song

        # Add current top song to the WordCloud text
        all_top_songs += " " + str(top_song)

        # Generate and update the gender plot
        gender_fig = plot_gender_chart(male_count, female_count)
        gender_plot_container.plotly_chart(gender_fig, key=f'gender_chart_{i}')

        # Generate and update the level plot
        level_fig = plot_level_chart(paid_count, free_count)
        level_plot_container.plotly_chart(level_fig, key=f'level_chart_{i}')

        # Generate and update the time zone map plot
        map_fig, max_timezone = plot_time_zone_map(row, i)
        map_plot_container.plotly_chart(map_fig, key=f'map_chart_{i}')

        # Display the timezone with the highest count below the map
        timezone_name_container.markdown(f"**Timezone with highest count: {max_timezone}**")

        # Generate and update the WordCloud for top songs
        wordcloud_fig = generate_wordcloud(all_top_songs)
        wordcloud_container.image(wordcloud_fig.to_array(), use_container_width=True)

        # Wait for a short time before updating to the next row
        time.sleep(2)  # Adjust this to the desired update interval

# Run the Streamlit app
if __name__ == "__main__":
    display_data()
