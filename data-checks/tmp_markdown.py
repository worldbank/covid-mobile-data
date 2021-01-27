import markdown


plot1 = '<iframe id="igraph" scrolling="no" style="border:none;" seamless="seamless" src="i1_count.html" height="525" width="100%"></iframe>'

foo = """

# This is a test

this is a test
<p style = "color: red"> this is a red test </p>


{}


""".format(plot1)

html = markdown.markdown(foo)

#------------------------------------
Html_file= open("test.html","w")
Html_file.write(html)
Html_file.close()


# foo = 42
# bar = "a"


# "frame: {} {}".format(foo, bar)