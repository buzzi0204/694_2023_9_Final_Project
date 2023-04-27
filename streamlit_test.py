import streamlit as st
from search_functions import get_hashtag, get_keyword, get_username, get_top_10_tweets, get_top_10_users

option = st.sidebar.selectbox('Search Query',['Hashtag', 'Keyword', 'Username/Full name'])

styles = [
    dict(selector="th", props=[("font-size", "12px"), ("text-align", "center")]),
    dict(selector="td", props=[("font-size", "12px"), ("text-align", "center")])]
#     dict(selector="th", 
#          props=[("font-size", "18px"), 
#                 ("text-align", "center"), 
#                 ("background-color", "#2196f3"), 
#                 ("color", "white")]),
#     dict(selector="td", 
#          props=[("font-size", "16px"), 
#                 ("text-align", "left"), 
#                 ("padding", "10px"), 
#                 ("background-color", "white"), 
#                 ("color", "black")]),
#     dict(selector=".row-hover:hover", 
#          props=[("background-color", "#f5f5f5")])
# ]

if option == 'Hashtag':
    input_text = st.sidebar.text_input("Enter hashtag")
    if st.sidebar.button("Submit"):
        with st.spinner('Getting data...'):
            data = get_hashtag(input_text)
            styled_data = data.style.set_table_styles(styles)
            st.table(styled_data)

if option == 'Keyword':
    input_text = st.sidebar.text_input("Enter keyword")
    if st.sidebar.button("Submit"):
        with st.spinner('Getting data...'):
            data = get_keyword(input_text)
            styled_data = data.style.set_table_styles(styles)
            st.table(styled_data)

if option == 'Username/Full name':
    input_text = st.sidebar.text_input("Enter username/full name")
    if st.sidebar.button("Submit"):
        with st.spinner('Getting data...'):
            data = get_username(input_text)
            styled_data = data.style.set_table_styles(styles)
            st.table(styled_data)

if st.sidebar.button("Top 10 Tweets"):
    data = get_top_10_tweets()
    styled_data = data.style.set_table_styles(styles)
    st.table(styled_data)

if st.sidebar.button("Top 10 Users"):
    data = get_top_10_users()
    styled_data = data.style.set_table_styles(styles)
    st.table(styled_data)