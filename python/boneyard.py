def senders():
    sql = ("select from_address from messages "
           "group by from_address having count(id) > 200 "
           "order by from_address collate nocase")

    records = self.database.query(request, sql)
    items = list()

    for record in records:
        address = record[0]
        href = self.sender_page.get_href(key=address)
        text = xml_escape(address)

        items.append(html_a(text, href))

    senders = html_ul(items, class_="three-column")

def send_sender(self, request):
    address = request.get("address")
    obj = Object(address, address)

    sql = ("select * from messages where from_address = ? "
           "order by date desc limit 1000")

    records = self.database.query(request, sql, address)
    message = Message()
    rows = list()

    for record in records:
        message.load_from_record(record)
        message_link = self.message_page.render_brief_link(message)

        row = [
            message_link,
            message.authored_words,
            xml_escape(str(_email.formatdate(message.date)[:-6])),
        ]

        rows.append(row)

    values = {
        "address": xml_escape(address),
        "messages": html_table(rows, False, class_="messages"),
    }

    content = _strings["sender"].format(**values)

    return self.sender_page.send_response(request, content, obj)
