import requests

from flask import request, Response

from common.urls import urls
from core.flask.sessions.session import AuthorizeUser

from common.mixin.mixin import server_response

from core.flask.decorators.decorators import check_token
from database.company_database.core.query_company import GetCompanyFromDatabase

from . import app


@app.route('/sage/<company_id>/routes/', methods=['GET', ])
@app.route('/<company_id>/live-pickup/', methods=['GET', ])
@check_token()
def get_active_routes_with_pickup_products(company_id):
    header = request.headers.get('Authorization')
    if not header:
        return server_response([], 403, 'Authorization failed. Please provide token.', False)

    validate_token = AuthorizeUser.verify_user_token(header)
    if not validate_token['success']:
        return server_response([], 403, validate_token['message'], False)

    check_company_assgment = GetCompanyFromDatabase.check_company_assignment(
        validate_token['response']['email'], company_id
    )

    if not check_company_assgment['success']:
        return server_response([], 403, "You don't have permissions for this operation.", False)

    if GetCompanyFromDatabase.routing_microservice_enabled(company_id):
        return server_response([], 401,
                               "This service is not enabled for users of new routing module. "
                               "Please, contact user support.", False)

    token = request.headers.get('Authorization')
    url = ("{cloud_domain}/api/v2/importer/{company_id}/live-pickup/?"
           "fromDateTime={from_date}&toDateTime={to_date}").format(
        cloud_domain=urls.url_path,
        company_id=company_id,
        from_date=request.args.get('fromDateTime', ''),
        to_date=request.args.get('toDateTime', '')
    )

    cloud_response = requests.get(
        url=url,
        headers={
            'Content-type': 'application/json',
            'Authorization': 'JWT %s' % token
        },
        timeout=120.00
    )

    return Response(response=cloud_response.text, status=cloud_response.status_code, content_type="application/json")


@app.route('/sage/<company_id>/prekitting-pickup/', methods=['GET', ])
@app.route('/<company_id>/prekitting-pickup/', methods=['GET', ])
@check_token()
def get_active_prekitting_routes_with_pickup_products(company_id):
    header = request.headers.get('Authorization')
    if not header:
        return server_response([], 403, 'Authorization failed. Please provide token.', False)

    validate_token = AuthorizeUser.verify_user_token(header)
    if not validate_token['success']:
        return server_response([], 403, validate_token['message'], False)

    check_company_assgment = GetCompanyFromDatabase.check_company_assignment(
        validate_token['response']['email'], company_id
    )

    if not check_company_assgment['success']:
        return server_response([], 403, "You don't have permissions for this operation.", False)

    if GetCompanyFromDatabase.routing_microservice_enabled(company_id):
        return server_response([], 403,
                               "This service is not enabled for users of new routing module. "
                               "Please, contact user support.", False)

    token = request.headers.get('Authorization')
    url = ("{cloud_domain}/api/v2/importer/{company_id}/prekitting-pickup/?"
           "fromDateTime={from_date}&toDateTime={to_date}").format(
        cloud_domain=urls.url_path,
        company_id=company_id,
        from_date=request.args.get('fromDateTime', ''),
        to_date=request.args.get('toDateTime', '')
    )

    cloud_response = requests.get(
        url=url,
        headers={
            'Content-type': 'application/json',
            'Authorization': 'JWT %s' % token
        },
        timeout=120.00
    )

    return Response(response=cloud_response.text, status=cloud_response.status_code, content_type="application/json")
