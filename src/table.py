from utils import with_cursors


# Formulas for the composite scores
formulas = {
	"score_stats": (
		"(`cheese_gathered` + `first` * 3) / POWER(round_played, 0.25)"
	),
	"score_shaman": "1",  # TODO
	"score_survivor": (
		"(1.6 * `{0}_survivor_count` + 0.8 * `{0}_mouse_killed`) "
		"/ POWER(`{0}_shaman_count` * `{0}_round_played`, 0.25)"
		.format("survivor")
	),
	"score_racing": (
		"(2 * `{0}_first` + `{0}_podium`) "
		"/ POWER(`{0}_round_played` * `{0}_finished_map`, 0.25)"
		.format("racing")
	),
	"score_defilante": (
		"`{0}_points` / POWER(`{0}_round_played` * `{0}_finished_map`, 0.25)"
		.format("defilante")
	),
	"score_overall": None,  # TODO
}


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
		self.write_columns = []
		self.composite_scores = []
		for row in await cursor.fetchall():
			if self.name == "player" and row[0].startswith("score_"):
				if row[0] == "score_overall":
					# This score is calculated post-download
					self.composite_scores.append(",1 as `{}`".format(row[0]))

				else:
					self.composite_scores.append(
						",{} as `{}`".format(formulas[row[0]], row[0])
					)
			else:
				self.columns.append(row[0])
			self.write_columns.append(row[0])

		self.composite_scores = "".join(self.composite_scores)

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
