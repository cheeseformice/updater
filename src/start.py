import asyncio
import logging
import aiomysql

from download import RunnerPool
from table import Table
from utils import env


logging.basicConfig(
	format='[%(asctime)s] [%(levelname)s] %(message)s',
	level=logging.DEBUG
)

try:
	import uvloop
except ImportError:
	uvloop = None
	logging.warning("Can't use uvloop.")


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
	tasks = []
	for pool in pools:
		pool.close()
		tasks.append(pool.wait_closed())

	loop.run_until_complete(asyncio.wait(*tasks))


if __name__ == "__main__":
	if uvloop is not None:
		uvloop.install()

	loop = asyncio.get_event_loop()

	pools = start(loop)
	try:
		run(loop, pools)
	finally:
		stop(loop, pools)
