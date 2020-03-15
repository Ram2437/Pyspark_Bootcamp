file = open("/Users/ramakrishnanimmathota/Research/Bootcamp/Local_ip/products/part-00000", "r")
display= 5
for x in range(display):
    lines = file.readline()
    print(lines)