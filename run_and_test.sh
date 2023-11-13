#! /usr/bin/env bash
sam build
sam local invoke --event events/search_by_state.json
sam local invoke --event events/search_by_zipcode.json