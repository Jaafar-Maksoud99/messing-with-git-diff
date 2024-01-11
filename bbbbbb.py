filepath = "second file.txt"

notebooks_to_deploy = []
notebooks_to_rename = []
notebooks_to_delete = []

with open(filepath, 'r') as file:
    for line in file:
        if line[:1] == "R":
            notebooks_to_rename.append(line.split("|")[1:])
        elif line[:1] == "D":
            notebooks_to_delete.append(line.split("|")[1])
        else:
            notebooks_to_deploy.append(line.split("|")[1])
print(notebooks_to_deploy)
print(notebooks_to_rename)
print(notebooks_to_delete)