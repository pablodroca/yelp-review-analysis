#!/bin/bash
docker build -f Dockerfile -t "reader:latest" .
docker run --volume /home/urielkelman/Escritorio/Facultad/Sistemas\ distribuidos\ I/TPS/TP2/yelp-review-analysis/data/reviews/:/data_reviews --volume /home/urielkelman/Escritorio/Facultad/Sistemas\ distribuidos\ I/TPS/TP2/yelp-review-analysis/data/business/:/data_business -e REVIEWS_PATH=/data_reviews/yelp_academic_dataset_reviews.json -e REVIEWS_MESSAGE_SIZE=4000 -e REVIEWS_ROUTING_KEY=reviews_queue -e BUSINESS_PATH=/data_business/yelp_academic_dataset_business.json -e BUSINESS_MESSAGE_SIZE=100 -e BUSINESS_ROUTING_KEY=business_queue -e EXCHANGE_INCOMING_BUSINESS=exchange_incoming_business -e EXCHANGE_INCOMING_REVIEWS=exchange_incoming_reviews -e FINAL_RESULTS_QUEUE=final_results_queue -e TOTAL_METRICS=5 --network yelp-review-analysis_pipeline_net reader:latest
