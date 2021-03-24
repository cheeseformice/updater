import uvloop
import asyncio
import logging
import aiomysql

from runner import RunnerPool
from table import Table
from utils import env


logging.basicConfig(
	format='[%(asctime)s] [%(levelname)s] %(message)s',
	level=logging.DEBUG
)


def start(loop):
	return loop.run_until_complete(asyncio.gather(
		# CFM DB
		aiomysql.create_pool(
			host=env.cfm_ip, port=3306,
			user=env.cfm_user, password=env.cfm_pass,
			db=env.cfm_db, loop=loop,
			autocommit=True
		),

		# Atelier801 API
		aiomysql.create_pool(
			host=env.a801_ip, port=3306,
			user=env.a801_user, password=env.a801_pass,
			db=env.a801_db, loop=loop
		),
	))


def run(loop, pools):
	runner = RunnerPool(100, 50, *pools)

	logging.debug("start all")
	loop.run_until_complete(asyncio.wait((
		runner.extract(Table("player")),
		runner.extract(Table("tribe")),
		runner.extract(Table("member")),
	)))
	logging.debug("end all")


def stop(loop, pools):
	for pool in pools:
		pool.close()

	loop.run_until_complete(asyncio.wait((
		*map(lambda pool: pool.wait_closed(), pools),
	)))


if __name__ == "__main__":
	uvloop.install()
	loop = asyncio.get_event_loop()

	pools = start(loop)
	try:
		run(loop, pools)
	finally:
		stop(loop, pools)
