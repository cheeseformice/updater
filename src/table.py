from utils import with_cursors


class Table:
	name: str = None
	primary: str = None
	columns: list = None
	is_empty: bool = False

	read_hash: str = None
	write_hash: str = None

	def __init__(self, name):
		self.name = name

	@with_cursors()
	async def extract_info(self, cursor, database):
		self.primary = "id"
		if self.name == "member":
			self.primary += "_member"  # ugly naming tig...

		# Get table columns
		await cursor.execute(
			"SELECT \
				`column_name` \
			FROM \
				`information_schema`.`columns` \
			WHERE \
				`table_schema`='{}' AND \
				`table_name`='{}'"
			.format(database, self.name)
		)
		self.columns = []
		for row in await cursor.fetchall():
			self.columns.append(row[0])

		# Check if the table is empty
		await cursor.execute(
			"SELECT \
				COUNT(*) \
			FROM \
				`{}`"
			.format(self.name)
		)
		row = await cursor.fetchone()
		await cursor.fetchone()  # has to return None so i can execute
		self.is_empty = row[0] == 0

		# Know which tables we are gonna use for hash cache
		hash_table = "{}_hashes_{{}}".format(self.name)
		self.read_hash = hash_table.format(0)
		self.write_hash = hash_table.format(1)

		# Truncate the write hash cache
		await cursor.execute(
			"TRUNCATE `{}`"
			.format(self.write_hash)
		)
