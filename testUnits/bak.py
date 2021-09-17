async def search(self):
    for elem in self.postdatas:
        # 常规字段检索语句构造
        if elem in ["store", "store_comments", "isbn", "book_name", "category",
                    "slogan", "book_description",
                    "catalogue", "languages" \
                , "word_count", "book_comments", "store_pricing", "selling_price", "publishing_house",
                    "edition", "impression" \
                , "inventory", "sales", "author", "shuppites", "format", "is_suit", "suits", "binding_layout",
                    "pages" \
                , "papers", "uploader", "selling_stores", "published_year_range", "published_year_integral",
                    "comments_range", "comments_integral", \
                    "premium_range", "premium_integral", "selling_stores_range", "selling_stores_integral",
                    "create_type", "data_source", "total_integral"]:
            datas = self.postdatas[elem]
            # 遍历前端传来的参数
            for data in datas:
                if data['value']:
                    # 针对text keyword混合字段进行特殊语句构造term语句所用的key
                    if elem in ["store", "book_name", "publishing_house", "author"]:
                        ielem = elem + "__keyword"
                    else:
                        ielem = elem
                    # 判断数字型字段输入字符串检索引发的错误，遇到即跳过
                    if elem in ["store_comments", "word_count", "book_comments", "store_pricing", "selling_price",
                                "inventory",
                                "sales", \
                                "suits", "pages", "selling_stores", "published_year_integral", "comments_integral", \
                                "premium_integral", "selling_stores_integral", "total_integral"]:
                        try:
                            try:
                                int(data['value'])
                            except Exception:
                                float(data['value'])
                        except Exception:
                            continue
                    # 匹配或语句
                    if data['logic'] == "|":
                        # 匹配模糊查询条件
                        if 'match' in data['condition']:
                            if not self.q2:
                                self.q2 = Q("match", **{elem: data['value']})
                            else:
                                self.q2 = Q("match", **{elem: data['value']}) | self.q2
                        else:
                            if not self.q2:
                                self.q2 = Q("term", **{ielem: data['value']})
                            else:
                                self.q2 = Q("term", **{ielem: data['value']}) | self.q2
                    # 匹配与条件
                    elif data['logic'] == "&":

                        if 'match' in data['condition']:
                            if not self.q1:
                                self.q1 = Q("match", **{elem: data['value']})
                            else:
                                self.q1 = Q("match", **{elem: data['value']}) & self.q1
                        else:

                            if not self.q1:
                                self.q1 = Q("term", **{ielem: data['value']})
                            else:
                                self.q1 = Q("term", **{ielem: data['value']}) & self.q1
                    # 匹配非条件
                    elif data['logic'] == "!":

                        if 'match' in data['condition']:
                            if not self.q3:
                                self.q3 = Q("match", **{elem: data['value']})
                            else:
                                self.q3 = Q("match", **{elem: data['value']}) | self.q3

                        else:

                            if not self.q3:
                                self.q3 = Q("term", **{ielem: data['value']})
                            else:
                                self.q3 = Q("term", **{ielem: data['value']}) | self.q3
                    # 其他非法情况跳过
                    else:
                        pass


        # source字段的检索语句构造
        elif elem in ["first_channel", "second_channel"]:
            datas = self.postdatas[elem]

            for data in datas:
                if data['value']:
                    if not self.q1:
                        self.q1 = Q("term", **{elem: data['value']})
                    else:
                        self.q1 = Q("term", **{elem: data['value']}) & self.q1

        # 任意匹配字段的检索语句构造
        elif elem in ["universe"]:
            datas = self.postdatas[elem]
            for data in datas:
                if data['value']:
                    # 如果输入的值是数字型则匹配全部字段，如果不是数字型则匹配部分字段
                    try:
                        try:
                            int(data['value'])
                        except Exception:
                            float(data['value'])
                        # match模糊查询所用的esl语句
                        match_seq = Q("multi_match", query=data['value'],
                                      fields=["first_channel", "second_channel", "store",
                                              "store_comments", "isbn", "book_name", "category",
                                              "slogan", "book_description", "catalogue", "languages" \
                                          , "word_count", "book_comments", "store_pricing", "selling_price",
                                              "publishing_house", "publishing_time", "printing_time",
                                              "edition", "impression" \
                                          , "inventory", "sales", "author", "shuppites", "format",
                                              "is_suit", "suits", "binding_layout", "pages" \
                                          , "papers", "uploader", "selling_stores",
                                              "published_year_range", "published_year_integral", "shelf_time",
                                              "sales_month",
                                              "comments_range", "comments_integral", \
                                              "premium_range", "premium_integral", "selling_stores_range",
                                              "selling_stores_integral", "create_type", "data_source",
                                              "total_integral"])
                        # term精确查询所用的esl语句
                        term_seq = Q("term", first_channel=data['value']) | Q("term",
                                                                              second_channel=data['value']) | Q(
                            "term", store__keyword=
                            data[
                                'value']) | Q(
                            "term", store_comments=data['value']) | Q("term", isbn=data['value']) | Q(
                            "term",
                            book_name__keyword=
                            data[
                                'value']) | Q(
                            "term", category=data['value']) | Q("term", slogan=data['value']) | Q(
                            "term",
                            book_description=
                            data[
                                'value']) | Q(
                            "term", catalogue=data['value']) | Q("term", languages=data['value']) | Q(
                            "term",
                            word_count=
                            data[
                                'value']) | Q(
                            "term", book_comments=data['value']) | Q("term", store_pricing=data['value']) | Q(
                            "term",
                            selling_price=data[
                                'value']) | Q(
                            "term", publishing_house__keyword=data['value']) | Q("term",
                                                                                 publishing_time=data[
                                                                                     'value']) | Q(
                            "term", printing_time=data['value']) | Q("term", edition=data['value']) | Q(
                            "term",
                            impression=
                            data[
                                'value']) | Q(
                            "term", inventory=data['value']) | Q("term", sales=data['value']) | Q("term",
                                                                                                  author__keyword=
                                                                                                  data[
                                                                                                      'value']) | Q(
                            "term", shuppites=data['value']) | Q("term", format=data['value']) | Q("term",
                                                                                                   is_suit=
                                                                                                   data[
                                                                                                       'value']) | Q(
                            "term", suits=data['value']) | Q("term", binding_layout=data['value']) | Q(
                            "term",
                            pages=data[
                                'value']) | Q(
                            "term", papers=data['value']) | Q("term", uploader=data['value']) | Q("term",
                                                                                                  selling_stores=
                                                                                                  data[
                                                                                                      'value']) | Q(
                            "term", published_year_range=data['value']) | Q("term",
                                                                            published_year_integral=data[
                                                                                'value']) | Q("term",
                                                                                              comments_range=
                                                                                              data[
                                                                                                  'value']) | Q(
                            "term", comments_integral=data['value']) | Q("term",
                                                                         premium_range=data['value']) | Q(
                            "term",
                            premium_integral=data[
                                'value']) | Q(
                            "term", selling_stores_range=data['value']) | Q("term",
                                                                            selling_stores_integral=data[
                                                                                'value']) | Q("term",
                                                                                              create_type=
                                                                                              data[
                                                                                                  'value']) | Q(
                            "term",
                            total_integral=
                            data[
                                'value']) | Q("term",
                                              shelf_time=
                                              data[
                                                  'value']) | Q("term",
                                                                sales_month=
                                                                data[
                                                                    'value'])
                    # 输入的不是数字型，则剔除数字型字段，以免引发搜索错误
                    except Exception as e:
                        match_seq = Q("multi_match", query=data['value'],
                                      fields=["first_channel", "second_channel", "store",
                                              "isbn", "book_name", "category",
                                              "slogan", "book_description", "catalogue", "languages", \
                                              "edition", "impression", \
                                              "author", "shuppites", "format",
                                              "is_suit", "binding_layout", \
                                              "papers", "uploader",
                                              "published_year_range",
                                              "comments_range", \
                                              "premium_range", "selling_stores_range",
                                              "create_type", "data_source"])

                        term_seq = Q("term", first_channel=data['value']) | Q("term",
                                                                              second_channel=data['value']) | Q(
                            "term",
                            store__keyword=
                            data[
                                'value']) | Q("term", isbn=data['value']) | Q(
                            "term",
                            book_name__keyword=
                            data[
                                'value']) | Q(
                            "term", category=data['value']) | Q("term", slogan=data['value']) | Q(
                            "term",
                            book_description=
                            data[
                                'value']) | Q(
                            "term", catalogue=data['value']) | Q("term", languages=data['value']) | Q(
                            "term", publishing_house__keyword=data['value']) | Q("term", edition=data['value']) | Q(
                            "term",
                            impression=
                            data[
                                'value']) | Q("term",
                                              author__keyword=
                                              data[
                                                  'value']) | Q(
                            "term", shuppites=data['value']) | Q("term", format=data['value']) | Q("term",
                                                                                                   is_suit=
                                                                                                   data[
                                                                                                       'value']) | Q(
                            "term", binding_layout=data['value']) | Q(
                            "term", papers=data['value']) | Q("term", uploader=data['value']) | Q(
                            "term", published_year_range=data['value']) | Q("term",
                                                                            comments_range=
                                                                            data[
                                                                                'value']) | Q("term",
                                                                                              premium_range=data[
                                                                                                  'value']) | Q(
                            "term", selling_stores_range=data['value']) | Q("term",
                                                                            create_type=
                                                                            data[
                                                                                'value'])

                    # 匹配或字段
                    if data['logic'] == "|":
                        # 全局模糊搜索
                        if 'match' in data['condition']:
                            if not self.q2:
                                self.q2 = match_seq
                            else:
                                self.q2 = match_seq | self.q2
                        # 全局精确搜索
                        elif 'term' in data['condition']:
                            if not self.q2:
                                self.q2 = term_seq
                            else:
                                self.q2 = term_seq | self.q2
                    # 匹配与字段
                    elif data['logic'] == "&":
                        # 全局模糊搜索
                        if 'match' in data['condition']:

                            if not self.q1:
                                self.q1 = match_seq

                            else:
                                self.q1 = match_seq & self.q1
                        # 全局精确搜索
                        elif 'term' in data['condition']:

                            if not self.q1:
                                self.q1 = term_seq
                            else:
                                self.q1 = term_seq & self.q1
                    # 匹配非字段
                    elif data['logic'] == "!":
                        # 全局模糊搜索
                        if 'match' in data['condition']:

                            if not self.q3:
                                self.q3 = match_seq
                            else:
                                self.q3 = match_seq | self.q3
                        # 全局精确搜索
                        elif 'term' in data['condition']:

                            if not self.q3:
                                self.q3 = term_seq
                            else:
                                self.q3 = term_seq | self.q3
                    else:
                        if not self.q1:
                            self.q1 = match_seq
                        else:
                            self.q1 = match_seq & self.q1


        # 日期范围检索语句构造
        elif elem in ["create_time", "update_time", "publishing_time", "sales_month", "shelf_time",
                      "printing_time"]:
            datas = self.postdatas[elem]

            for data in datas:
                # #根据前端传入得年份，扩大其搜索范围
                # if elem in ['publishing_time'] and '-' in data['value'][0]:
                #     data['value'][0] = data['value'][0] + '-01-01'
                #     data['value'][1] = data['value'][1] + "-12-31"
                if not self.q1:
                    self.q1 = Q('range', **{elem: {'gte': data['value'][0], 'lte': data['value'][1]}})
                else:
                    self.q1 = Q('range', **{elem: {'gte': data['value'][0], 'lte': data['value'][1]}}) & self.q1


##################################################
