import logging

from table import Table

from utils import env


async def post_update(player, tribe, member, cfm, a801):
	async with cfm.acquire() as conn:
		async with conn.cursor() as inte:
			return await _post_update(player, tribe, member, inte)


async def _post_update(player, tribe, member, inte):
	stats = Table("tribe_stats")
	# Extract stats info
	await stats.extract_info(inte, env.cfm_db, hashes=False)

	if not tribe.is_empty:
		logging.debug("[tribe] calculating active tribes")

		await inte.execute("TRUNCATE `tribe_active`")
		await inte.execute(
			"INSERT INTO `tribe_active` \
				(`id`, `member`, `active`, `member_sqrt`) \
			\
			SELECT \
				`t`.`id`, \
				COUNT(`m`.`id_member`) as `member`, \
				COUNT(`p_n`.`id`) as `active`, \
				POWER(COUNT(`m`.`id_member`), 0.5) as `member_sqrt` \
			FROM \
				`tribe` as `t` \
				INNER JOIN `member` as `m` \
					ON `t`.`id` = `m`.`id_tribe` \
				INNER JOIN `player_new` as `p` \
					ON `m`.`id_member` = `p`.`id` \
			GROUP BY `t`.`id`"
		)

	logging.debug("[tribe] calculating stats")

	# Prepare query
	if tribe.is_empty:
		columns = [
			"COUNT(`m`.`id_member`) as `member`",
			"COUNT(`p_n`.`id`) as `active`",
		]
		div_by = "POWER(COUNT(`m`.`id_member`), 0.5)"
	else:
		columns = [
			"`t`.`member`",
			"`t`.`active`",
		]
		div_by = "`t`.`member_sqrt`"

	for column in stats.columns:
		if column not in (
			"id",
			"member",
			"active",
		):
			columns.append(
				"SUM(`p`.`{0}`) / {1} as `{0}`"
				.format(column, div_by)
			)

	# Run query
	await inte.execute(
		"REPLACE INTO `tribe_stats` \
		SELECT \
			`t`.`id`, \
			{0} \
		FROM \
			`tribe{1}` as `t` \
			INNER JOIN `member` as `m` \
				ON `t`.`id` = `m`.`id_tribe` \
			INNER JOIN `player` as `p` \
				ON `p`.`id` = `m`.`id_member` \
			{2} \
		GROUP BY `t`.`id`"
		.format(
			",".join(columns),
			"" if tribe.is_empty else "_active",

			"LEFT JOIN `player_new` as `p_n` ON `p_n`.`id` = `p`.`id`"
			if tribe.is_empty else
			# No need to join player_new if we are using tribe_active
			""
		)
	)
