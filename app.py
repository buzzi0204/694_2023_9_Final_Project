import streamlit as st

apptitle = "Tweet Search Application"

st.set_page_config(page_title=apptitle, page_icon='🔎')

st.title('Tweet Search Application')

st.markdown("""
 * Use the menu at left to select the search value
 * Your results will appear below
""")

st.sidebar.markdown("Select search condition")

select_event = st.sidebar.selectbox('What do you wanna find?',
                                    ['Username','Keyword','Hashtag'])

st.sidebar.markdown('Search')
search_event = st.sidebar.text_input("Enter your query")


button_clicked = st.sidebar.button('Submit')

st.sidebar.markdown('')
st.sidebar.markdown('')

st.sidebar.markdown('Click on each button below to get:')

top10_user = st.sidebar.button('Top10 Usernames')
top10_tweets = st.sidebar.button('Top10 Tweets')

# Disable Streamlit caching
select_event = st.cache(allow_output_mutation=True)(select_event)
search_event = st.cache(allow_output_mutation=True)(search_event)
button_clicked = st.cache(allow_output_mutation=True)(button_clicked)
top10_user = st.cache(allow_output_mutation=True)(top10_user)
top10_tweets = st.cache(allow_output_mutation=True)(top10_tweets)
`