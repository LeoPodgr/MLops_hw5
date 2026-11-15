.PHONY: check up down logs ps publish-model-demo demo-batch

check:
	python tools/validate_project.py
	python -m unittest discover -s tests -v
	python -m compileall -q services/src tools

down:
	docker compose down

up:
	docker compose up -d --build

logs:
	docker compose logs -f --tail=100 fraud-detector scoring-writer transaction-simulator

ps:
	docker compose ps

publish-model-demo:
	python tools/publish_model.py --bias 3.0

demo-batch:
	docker compose exec fraud-detector python /app/tools/publish_demo_batch.py --count 200 --amount 2500 --state NY --merchant demo_high_risk_merchant --category electronics
