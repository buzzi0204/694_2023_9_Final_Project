import streamlit as st
import base64

def local_css(file_name):
    with open(file_name) as f:
        st.markdown(f'<style>{f.read()}</style>', unsafe_allow_html=True)

def remote_css(url):
    st.markdown(f'<link href="{url}" rel="stylesheet">', unsafe_allow_html=True)    

def icon(icon_name):
    st.markdown(f'<i class="material-icons">{icon_name}</i>', unsafe_allow_html=True)

local_css("style.css")
remote_css('https://fonts.googleapis.com/icon?family=Material+Icons')


def add_bg_from_local(image_file):
    with open(image_file, "rb") as image_file:
        encoded_string = base64.b64encode(image_file.read())
    st.markdown(
        f"""
        <style>
        .stApp {{
            background-image: linear-gradient(
                rgba(0, 0, 0, 0.5),
                rgba(0, 0, 0, 0.5)
              ), url(data:image/{"png"};base64,{encoded_string.decode()});
            background-size: cover;
        }}
        .css-1g7kzw2.e1ehc6vh0 {{
            background-color: transparent !important;
            box-shadow: none !important;
        }}
        </style>
        """,
        unsafe_allow_html=True
    )

add_bg_from_local('twitter.png')    

col1, col2 = st.columns([1, 9])
with col1:
    icon("search")
    st.markdown(
        """<style>
           i.material-icons {
              color: white;
              padding: 30px;
           }
           </style>
        """,
        unsafe_allow_html=True
    )
with col2:
    st.markdown(
        """<style>
           h3 {
              color: white;
           }
           </style>
        """,
        unsafe_allow_html=True
    )
    st.markdown("<h3>Twitter Searcher Pro Maxx</h3>", unsafe_allow_html=True)

selected = st.text_input("", "Search...")
button_clicked = st.button("OK")
