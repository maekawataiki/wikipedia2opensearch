# %%
import pandas as pd
import time
import wikipediaapi
from typing import List

# %%

wiki = wikipediaapi.Wikipedia('ja')
movies_list_file = "data/movies.csv"
articles_file = "data/articles.csv"

# %% Retrieve list of Movies

def get_movies(year: int) -> List[str]:
    """
    Filter List of movies from category page
    """
    page = wiki.page(f'Category:{year}年の映画')
    skip_keywords = [
        "Category:",  # PoC につきたどるのは最初の階層のみ
        "Template:",
        "映画一覧",
        "映画の一覧",
        "国際映画祭"
    ]
    skip_titles = [
        f"{year}年の日本公開映画",
        f"{year}年の映画"
    ]
    for k, v in page.categorymembers.items():
        if any(keyword in k for keyword in skip_keywords):
            print(k)
            continue
        if any(title == k for title in skip_titles):
            print(k)
            continue
        yield v


movies = []
# テストにつき2010以降のみ
for year in range(2010, 2023):
    movies += get_movies(year)
    time.sleep(1)
print(len(movies))

# %% Export List of Titles

titles = [movie.title for movie in movies]
pd.DataFrame(titles, columns=['title']).to_csv(movies_list_file, index=False)

# %% Retrieve movies

movies = pd.read_csv(movies_list_file)['title']

# %% Retrieve text from movies

def get_movie_article(title: str) -> object:
    """
    Get article from wikipedia
    """
    page = wiki.page(title)
    return {'title': title, 'content': page.text}

result_list = []
for idx, title in enumerate(movies):
    result_list += [get_movie_article(title)]
    time.sleep(0.5)
    if idx % 100 == 0:
        print(idx)

# %% Export CSV

pd.DataFrame(result_list).to_csv(articles_file, index=False)


# %%

print(len(result_list))
# %%
