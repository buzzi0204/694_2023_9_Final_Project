import streamlit as st
from search_functions import get_hashtag, get_keyword, get_username, get_top_10_tweets, get_top_10_users

option = st.sidebar.selectbox('Search Query',['Hashtag', 'Keyword', 'Username/Full name'])

if option == 'Hashtag':
    input_text = st.sidebar.text_input("Enter hashtag")
    if st.sidebar.button("Submit"):
        with st.spinner('Getting data...'):
            data = get_hashtag(input_text)
        st.write(data)

if option == 'Keyword':
    input_text = st.sidebar.text_input("Enter keyword")
    if st.sidebar.button("Submit"):
        with st.spinner('Getting data...'):
            data = get_keyword(input_text)
        st.write(data)

if option == 'Username/Full name':
    input_text = st.sidebar.text_input("Enter username/full name")
    if st.sidebar.button("Submit"):
        with st.spinner('Getting data...'):
            data = get_username(input_text)
        st.write(data)

if st.sidebar.button("Top 10 Tweets"):
    data = get_top_10_tweets()
    st.table(data)

if st.sidebar.button("Top 10 Users"):
    data = get_top_10_users()
    st.table(data)