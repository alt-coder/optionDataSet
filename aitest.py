from duckduckgo_search import DDGS

results = DDGS().text("Crew AI advantages and improvements required", max_results=5)
print(results['href'])