import sqlite3
import argparse
import pandas as pd

def create_connection(db_file):
    """ create a database connection to the SQLite database
        specified by the db_file
    :param db_file: database file
    :return: Connection object or None
    """
    conn = None
    try:
        conn = sqlite3.connect(db_file)
    except Error as e:
        print(e)
    return conn

def search_category(db, category_name):
    cur = db.cursor()
    cur.execute("""select cl_from, cl_type, name1, name2, pos from categorylinks INNER JOIN index_title_lookup ON categorylinks.cl_from=index_title_lookup.id WHERE cl_to="{}";""".format(category_name))
    rows = cur.fetchall()
    subcategories = []
    page_titles = []
    page_ids = []
    page_offsets = []
    for wiki_id, page_type, name1, name2, pos in rows:
        if(page_type=='subcat'):
            subcategories.append(name2)
        elif(page_type=='page'):
            page_ids.append(wiki_id)
            page_titles.append(name1)
            page_offsets.append(pos)
    return subcategories, page_titles, page_ids, page_offsets

def recursive_search(db, root_name, max_len):
    searched_categories = []
    page_titles = []
    page_ids = []
    page_offsets = []
    categories = [root_name]
    while(len(categories) != 0 and len(page_ids) < max_len):
        category = ''
        while(category in searched_categories):
            category = categories.pop(0)
        print('Searching {}'.format(category))
        subcategories, new_page_titles, new_page_ids, new_page_offsets = search_category(db, category.replace(' ', '_'))
        searched_categories.append(category)
        categories = categories + subcategories        
        page_titles = page_titles + new_page_titles
        page_ids = page_ids + new_page_ids
        page_offsets = page_offsets + new_page_offsets
    return page_titles, page_ids, page_offsets

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Search for wiki all pages cointained on category (and its subcategories).')
    parser.add_argument('--category', default='Medicine', type=str)
    parser.add_argument('--max_len', default=300000, type=int)
    args = parser.parse_args()
    print('Creating connection with db')
    db = create_connection('wiki.db')
    page_titles, page_ids, page_offsets = recursive_search(db, args.category, args.max_len)
    print(page_titles)
    df = pd.DataFrame({'id' : page_ids, 'title' : page_titles, 'offset' : page_offsets})
    df.to_csv(args.category + '_pages.csv')
