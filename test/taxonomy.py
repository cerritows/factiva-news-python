from factiva.news import Taxonomy

taxonomy = Taxonomy()
print(taxonomy.categories)
industries_code = taxonomy.get_category_codes('industries')
print(industries_code)
company = taxonomy.get_company('isin', company_code='PLUNMST00014')
code_list = ["US0378331005", "US0231351067", "US5949181045", "US4523531083"]
print(company)
companies = taxonomy.get_company('isin', companies_codes=code_list)
print(companies)
print('Done!')
