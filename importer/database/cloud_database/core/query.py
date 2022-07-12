import re

from common.logging.setup import logger
from common.mixin.enum_errors import roles_support, PlanogramEnum
from common.mixin.mixin import make_response
from core.flask.sessions.session import AuthorizeUser
from database.cloud_database.common.common import get_cloud_connection_safe
from sqlalchemy import outerjoin, join, and_, literal_column, func, or_
from sqlalchemy.orm import aliased
from sqlalchemy.sql import select, update
from decimal import Decimal
from database.cloud_database.models.models import (
    user, custom_user, company, location, region,
    machine_type, machine, user_role, client, client_type,
    machine_data, operation, product, tax_rate,
    tags, machines_tags, locations_working_time,
    time_interval, machine_column, layout_column,
    product_template, product_rotation_group,
    product_category, machine_cluster, machine_assigned_cluster, region_parent, device, device_type,
    device_machine,
    packing_name, product_packing_size,
    vends, product_templates, recipe, combo_recipe, layout_components, layout_columns,
    layout_columns_tags, meter_type, machine_operation, company_access, warehouse, machine_category,
    product_rotation_groups,
    product_rotation_assignments, machine_recipes, product_components, product_component_items)


class CustomUserQueryOnCloud(object):

    @classmethod
    def get_auth_token_from_user_id(cls, user_id):

        with get_cloud_connection_safe() as conn_cloud:
            user_query = select([user]).where(user.id == user_id)
            result = conn_cloud.execute(user_query)

            user_query_data = result.fetchone()

            if user_query_data is None:
                return make_response(False, [], 'User does not exists with email: %s' % user_id)
            if not user_query_data.is_active:
                return make_response(False, [], 'User is not active: %s' % user_id)

            user_details = select([custom_user]).where(custom_user.user_ptr_id == user_query_data.id)
            result_details = conn_cloud.execute(user_details)

            result_user = result_details.fetchone()

            if result_user is None:
                return {'status': False, 'result': [], 'message': 'User company not found.'}

            # Q user role
            roles = select([user_role]).where(user_role.id == result_user.user_role_id)
            fetch_role = conn_cloud.execute(roles)

            data_role = fetch_role.fetchone()

            if data_role.name not in roles_support:
                return {'status': False, 'result': [], 'message': "You don't have access right."}

            user_data = [
                {
                    'id': user_query_data.id,
                    'email': user_query_data.email,
                    'password': user_query_data.password,
                    'company_id': result_user.company_id,
                    'full_name': '{} {}'.format(user_query_data.first_name, user_query_data.last_name),
                    'username': user_query_data.username,
                    'language': result_user.preferred_language
                 }
            ]

            token = AuthorizeUser.generate_jwt_token(user_data)
            return {'status': True, 'token': 'JWT ' + str(token.decode('ascii'))}

    @classmethod
    def check_if_user_exists_on_database(cls, email):

        with get_cloud_connection_safe() as conn_cloud:

            user_query = select([user]).where(user.email == email)
            result = conn_cloud.execute(user_query)

            user_query_data = result.fetchone()

            if user_query_data is None:
                return make_response(False, [], 'User does not exists with email: %s' % email)
            if not user_query_data.is_active:
                return make_response(False, [], 'User is not active: %s' % email)

            user_details = select([custom_user]).where(custom_user.user_ptr_id == user_query_data.id)
            result_details = conn_cloud.execute(user_details)

            result_user = result_details.fetchone()

            if result_user is None:
                return {'status': False, 'result': [], 'message': 'User company not found.'}

            logger.info('User login request: {}'.format(result_user))

            # Q user role
            roles = select([user_role]).where(user_role.id == result_user.user_role_id)
            fetch_role = conn_cloud.execute(roles)

            data_role = fetch_role.fetchone()

            user_data = [
                {
                    'id': user_query_data.id,
                    'email': email,
                    'password': user_query_data.password,
                    'company_id': result_user.company_id,
                    'full_name': '{} {}'.format(user_query_data.first_name, user_query_data.last_name),
                    'username': user_query_data.username,
                    'language': result_user.preferred_language
                }
            ]

            if data_role.name in roles_support or data_role.importer_edit:
                if data_role.name in roles_support[:2]:
                    return make_response(True, user_data, 'User found with email: %s' % email)
                elif data_role.name in roles_support[2:] and data_role.importer_edit:
                    return make_response(True, user_data, 'User found with email: %s' % email)
                else:
                    return {'status': False, 'result': [], 'message': "You don't have access right."}
            else:
                return {'status': False, 'result': [], 'message': "You don't have access right."}


class CompanyQueryOnCloud(object):

    @classmethod
    def get_company_by_id(cls, id):
        """
        Supposed to be used for company syncing prior to import. Gets the import configuration
        :param id: Cloud ID of the company for which import is done
        :return: alchemy company object from cloud database
        """
        with get_cloud_connection_safe() as conn_cloud:
            company_query = select([company]).where(
                (company.id == id) & (company.enabled.is_(True)) & (company.alive.is_(True)))

            result = conn_cloud.execute(company_query)

            company_data = result.fetchone()
            if company_data is None:
                return make_response(False, [], 'Company not found')

            user_data = [{
                'id': company_data.id,
                'company': company_data,
                'company_timezone': company_data.timezone
            }]

        return make_response(True, user_data, 'Company found with id: {}'.format(company_data.id))

    @classmethod
    def get_company_meter_readings_by_id(cls, id):
        """

        :param id: Cloud ID of the company for which import is done
        :return: True/False
        """
        with get_cloud_connection_safe() as conn_cloud:
            company_query = select([company_access]).where(
                company.id == id).select_from(outerjoin(company, company_access, company.access_id==company_access.id))
            result = conn_cloud.execute(company_query)

            company_access_data = result.fetchone()
            if company_access_data is None:
                return make_response(False, False, 'Company not found')

        return make_response(True, company_access_data.meter_readings, 'Meter readings : {}'.format(company_access_data))


class MachineQueryOnCloud(object):
    @classmethod
    def get_machines_by_external_ids(cls, company_id, external_ids, alive=True, extra=False, with_devices=False):
        if len(external_ids)==0:
            return make_response(False, [], 'Machine not found')

        with get_cloud_connection_safe() as conn_cloud:
            search_ids = external_ids if isinstance(external_ids, (list, tuple, set)) else [external_ids]


            if with_devices:
                machines_query = (select([machine, device.id.label('device_id'), device_type.type.label('device_type'), device.active.label('device_active')]).
                    where(and_(machine.owner_id == int(company_id),
                                machine.external_id.in_(search_ids) if not extra else machine.external_id.notin_(search_ids),
                                machine.alive.is_(alive))).
                    select_from(outerjoin(outerjoin(outerjoin(machine, device_machine, device_machine.machine_id == machine.id),
                                     device, device_machine.device_id == device.id),
                                     device_type, device_type.id == device.device_type_id
                                          ))
                        )


            else:
                machines_query = select([machine]).where(and_(
                    machine.owner_id == int(company_id),
                    machine.external_id.in_(search_ids) if not extra else machine.external_id.notin_(search_ids),
                    machine.alive.is_(alive)
                )
                )

            found_machines = conn_cloud.execute(machines_query).fetchall()
            if found_machines is None:
                return make_response(False, [], 'Machine not found')

            if with_devices:
                user_data = [dict(id=m.id, ext_id=m.external_id, name=m.caption, device_id=m.device_id, device_type=m.device_type, device_active=m.device_active) for m in found_machines]
            else:
                user_data = [dict(id=m.id, ext_id=m.external_id, name=m.caption) for m in found_machines]

        return make_response(True, user_data, 'Found num machines: {}'.format(len(user_data)))

    @classmethod
    def get_machines_for_company(cls, company_id):
        with get_cloud_connection_safe() as conn_cloud:

            machine_cluster_external_id = func.string_agg(
                machine_cluster.external_id, literal_column("'#'")
            )

            machines_query = (
                select([
                    machine.id.label('id'),
                    machine.external_id.label('external_id'),
                    machine.caption.label('caption'),
                    machine.alive.label('alive'),
                    machine.location_id.label('location_id'),
                    machine.type_id.label('type_id'),
                    machine_cluster_external_id.label("cluster_id"),
                    machine_operation.meter_readings.label("meter_readings")
                ])
                .where(machine.owner_id == int(company_id))
                .select_from(
                    outerjoin(machine, machine_assigned_cluster, machine.id == machine_assigned_cluster.machine_id)
                    .outerjoin(machine_cluster, machine_assigned_cluster.machinecluster_id == machine_cluster.id)
                        .join(machine_operation, machine.id == machine_operation.machine_id)
                )
                .group_by(
                    machine.id,
                    machine.external_id,
                    machine.caption,
                    machine.alive,
                    machine.location_id,
                    machine.type_id,
                    machine_operation.meter_readings
                )
            )

            found_machines = conn_cloud.execute(machines_query).fetchall()
            if found_machines is None:
                return make_response(False, [], 'Machine not found')

            user_data = [dict(
                id=m.id,
                ext_id=m.external_id,
                name=m.caption,
                alive=m.alive,
                location_id=m.location_id,
                type_id=m.type_id,
                cluster_id=m.cluster_id,
                meter_readings=m.meter_readings
            ) for m in found_machines]

        return make_response(True, user_data, 'Found num machines: {}'.format(len(user_data)))

    @classmethod
    def delete_machine_on_cloud(cls, company_id, external_id):
        with get_cloud_connection_safe() as conn_cloud:

            machines_query = update(machine).where(
                (machine.owner_id == company_id) &
                (machine.external_id == external_id) &
                (machine.alive.is_(True))
            ).values(alive=False)
            cnt = conn_cloud.execute(machines_query).rowcount

        return cnt

    @classmethod
    def export_machines(cls, company_id):
        with get_cloud_connection_safe() as conn_cloud:

            tags_captions = func.string_agg(
                tags.caption, literal_column("','")
            )

            mc_external_id_query = (
                select([
                    machine.external_id.label("machine_id"),
                    machine.caption.label("caption"),
                    machine_cluster.external_id.label("cluster_id"),
                    machine_cluster.company_id.label("company_id"),
                ])
                .where(and_(
                    machine.owner_id == int(company_id),
                    machine_cluster.company_id == int(company_id),
                    machine.id == machine_assigned_cluster.machine_id,
                    machine_cluster.id == machine_assigned_cluster.machinecluster_id
                ))
            )

            mcs_for_machines = conn_cloud.execute(mc_external_id_query).fetchall()

            machines_query = (
                select([
                    machine.caption.label('machine_name'),
                    machine.external_id.label('machine_id'),
                    literal_column('1').label('machine_action'),
                    location.external_id.label('machine_location_id'),
                    machine_type.code.label('machine_type_id'),
                    client.external_id.label('client_id'),
                    func.to_char(machine_data.installation_date, 'YYYY-MM-DD').label('installation_date'),
                    machine_data.model_number,
                    tags_captions.label('tags'),
                    machine_data.description.label('description'),
                    machine.recommended_visit_after.label('recommended_visit'),
                    machine.urgent_visit_after.label('urgent_visit'),
                    operation.undefined_column_warning.label('raise_event_on_undefined_column'),
                    operation.routing,
                    operation.stock_tracking,
                    operation.events_tracking,
                    operation.no_sales_window.label('no_cash_alarm'),
                    operation.no_sales_window_cashless.label('no_cashless_1_alarm'),
                    operation.no_sales_window_cashless_2.label('no_cashless_2_alarm'),
                    operation.no_sales_window_cashless_3.label('no_cashless_3_alarm'),
                    operation.no_sales_window_any.label('no_sales_alarm'),
                    warehouse.external_id.label('location_warehouse_id'),
                    machine_category.external_id.label('machine_category_id')
                ])
                .where(and_(
                    machine.owner_id == int(company_id),
                    machine.alive.is_(True)
                ))
                .select_from(
                    outerjoin(machine, location, machine.location_id == location.id)
                    .outerjoin(machine_type, machine.type_id == machine_type.id)
                    .outerjoin(client, machine.client_id == client.id)
                    .outerjoin(machine_data, machine.data_id == machine_data.id)
                    .outerjoin(operation, machine.id == operation.machine_id)
                    .outerjoin(machines_tags, machine.id == machines_tags.machine_id)
                    .outerjoin(tags, machines_tags.tags_id == tags.id)
                    .outerjoin(warehouse, machine.warehouse_id == warehouse.id)
                    .outerjoin(machine_category, machine.category_id == machine_category.id)
                )
                .group_by(
                    machine.caption,
                    machine.external_id,
                    location.external_id,
                    machine_type.code,
                    client.external_id,
                    machine_data.installation_date,
                    machine_data.model_number,
                    machine_data.description,
                    machine.recommended_visit_after,
                    machine.urgent_visit_after,
                    operation.undefined_column_warning,
                    operation.routing,
                    operation.stock_tracking,
                    operation.events_tracking,
                    operation.no_sales_window,
                    operation.no_sales_window_cashless,
                    operation.no_sales_window_cashless_2,
                    operation.no_sales_window_cashless_3,
                    operation.no_sales_window_any,
                    warehouse.external_id,
                    machine_category.external_id
                )
            )

            machine_query = conn_cloud.execute(machines_query)

            results = []
            for x in machine_query:
                dict_for_insert = dict(zip(x.keys(), x.values()))
                for k in dict_for_insert.keys():
                    if 'alarm' in k:
                        val = dict_for_insert.get(k, 0)
                        val = int(val) if val else 0
                        val = '{:02d}:{:02d}'.format(val // 60, val % 60)
                        dict_for_insert[k] = val
                    # let Boolean fields be integer
                    if k in ['events_tracking', 'raise_event_on_undefined_column', 'routing', 'stock_tracking']:
                        value = dict_for_insert.get(k)
                        value = 1 if value else 0
                        dict_for_insert[k] = value

                # search machinecluster
                mcs_found = [mcs.cluster_id for mcs in mcs_for_machines if mcs.machine_id == x.machine_id and mcs.company_id == int(company_id) and mcs.caption == x.machine_name]
                dict_for_insert['cluster_id'] = mcs_found[0] if len(mcs_found) else None

                results.append(dict_for_insert)

        return sorted(results, key=lambda name: name["machine_name"])

    @classmethod
    def get_meter_type_keys(cls, company_id):
        with get_cloud_connection_safe() as conn_cloud:

            meter_type_query = (
                select([
                    meter_type.external_id
                ])
                .where(and_(
                    meter_type.owner_id == int(company_id),
                    meter_type.alive == True
                ))
            )

            meter_types_response = conn_cloud.execute(meter_type_query)
            meter_type_external_ids = []
            for mt_response in meter_types_response:
                meter_type_external_ids.append(mt_response['external_id'])
            return meter_type_external_ids

class LocationQueryOnCloud(object):

    @classmethod
    def get_locations_by_external_ids(cls, company_id, external_ids, alive=True, extra=False):
        with get_cloud_connection_safe() as conn_cloud:
            search_ids = external_ids if isinstance(external_ids, (list, tuple, set)) else [external_ids]
            locations_query = select([location]).where(and_(
                location.owner_id == int(company_id),
                location.external_id.in_(search_ids) if not extra else location.external_id.notin_(external_ids),
                location.alive.is_(alive))
            )
            found_locations = conn_cloud.execute(locations_query).fetchall()

            if found_locations is None:
                return make_response(False, [], 'Location not found')

            user_data = [dict(
                id=l.id,
                ext_id=l.external_id,
                name=l.caption,
                alive=l.alive
            ) for l in found_locations]

        return make_response(True, user_data, 'Found num locations: {}'.format(len(user_data)))

    @classmethod
    def get_locations_for_company(cls, company_id):
        with get_cloud_connection_safe() as conn_cloud:
            locations_query = select([location]).where(location.owner_id == int(company_id))
            found_locations = conn_cloud.execute(locations_query).fetchall()

            if found_locations is None:
                return make_response(False, [], 'Location not found')

            user_data = [dict(
                id=l.id,
                ext_id=l.external_id,
                name=l.caption,
                alive=l.alive
            ) for l in found_locations]

        return make_response(True, user_data, 'Found num locations: {}'.format(len(user_data)))


    @classmethod
    def get_region_id_and_location(cls, company_id):
        with get_cloud_connection_safe() as conn_cloud:

            location_query = select(
                [
                    location.caption.label('location_name'),
                    location.external_id.label('location_id'),
                    region.external_id.label('region_id'),
                ]
            ).where(
                and_(
                    location.owner_id == company_id,
                    location.alive.is_(True)
                )
            ).select_from(
                outerjoin(location, locations_working_time,
                          location.id == locations_working_time.machinelocation_id
                )
                .outerjoin(
                    time_interval,
                    and_(time_interval.id == locations_working_time.timeinterval_id,
                         time_interval.alive.is_(True))
                ).outerjoin(
                    region,
                    region.id == location.cluster_id
                )
            ).group_by(
                location.caption,
                location.external_id,
                location.cluster_id,
                region.external_id
            )

            run_query = conn_cloud.execute(location_query)
            output_results = []

            for sort in run_query:
                output_results.append(dict(zip(sort.keys(), sort.values())))

        return sorted(output_results, key=lambda name: name["location_name"])

    @classmethod
    def machine_cluster_id(cls, mc_external_id, company_id):
        with get_cloud_connection_safe() as conn_cloud:

            mc_query = select(
                [
                    machine_cluster.caption.label('machine_cluster_name'),
                    machine_cluster.external_id.label('machine_cluster_id'),
                ]
            ).where(
                and_(
                    machine_cluster.external_id == mc_external_id,
                    machine_cluster.company_id == company_id
                )
            )

            output_result = conn_cloud.execute(mc_query).fetchone()

        if output_result:
            return output_result.machine_cluster_id

        return None


    @classmethod
    def export_locations(cls, company_id):

        with get_cloud_connection_safe() as conn_cloud:

            start_time = func.string_agg(
                func.to_char(time_interval.start_time, 'HH24:MI'), literal_column("'-'")
            )
            end_time = func.string_agg(
                func.to_char(time_interval.end_time, 'HH24:MI'), literal_column("'-'")
            )

            pre_start = start_time
            pre_end = end_time

            location_query = select(
                [
                    location.caption.label('location_name'),
                    location.external_id.label('location_id'),
                    region.external_id.label('region_id'),
                    location.longitude.label('longitude'),
                    location.latitude.label('latitude'),
                    location.phone.label('phone'),
                    location.email.label('email'),
                    pre_start.label('key_start'),
                    pre_end.label('key_end'),
                    location.address.label('location_address'),
                    location.note.label('description'),
                    location.weekday_mask.label('working_days'),
                    literal_column('1').label('location_action'),
                ]
            ).where(
                and_(
                    location.owner_id == company_id,
                    location.alive.is_(True)
                )
            ).select_from(
                outerjoin(location, locations_working_time,
                          location.id == locations_working_time.machinelocation_id
                )
                .outerjoin(
                    time_interval,
                    and_(time_interval.id == locations_working_time.timeinterval_id,
                         time_interval.alive.is_(True))
                ).outerjoin(
                    region,
                    region.id == location.cluster_id
                )
            ).group_by(
                location.caption,
                location.external_id,
                location.cluster_id,
                location.longitude,
                location.latitude,
                location.phone,
                location.email,
                location.note,
                location.weekday_mask,
                location.cluster_id,
                location.address,
                region.external_id
            )


            run_query = conn_cloud.execute(location_query)
            output_results = []

            for sort in run_query:
                output_results.append(dict(zip(sort.keys(), sort.values())))

            for x in output_results:
                for key, value in list(x.items()):
                    if key == 'key_start':
                        if value and len(value):
                            gt = x.get('key_start')
                            lt = x.get('key_end')

                            sp1 = gt.split('-')
                            sp2 = lt.split('-')

                            var = ''
                            for z, y in zip(sp1, sp2):
                                if len(gt) > 5:
                                    var += z + '-' + y + '#'
                                else:
                                    var += z + '-' + y
                            if len(var) > 11:
                                x['working_hours'] = var[:-1]
                            else:
                                x['working_hours'] = var

                            x.pop('key_start')
                            x.pop('key_end')
                        else:
                            x['working_hours'] = None
                            x.pop('key_start')
                            x.pop('key_end')

        return sorted(output_results, key=lambda name: name["location_name"])


class RegionQueryOnCloud(object):

    @classmethod
    def get_region_by_external_id(cls, company_id, region_id):
        with get_cloud_connection_safe() as conn_cloud:
            regions_query = select([region]).where(and_(
                region.owner_id == int(company_id), region.external_id == str(region_id),
                region.alive.is_(True))
            )
            found_region = conn_cloud.execute(regions_query).fetchone()

            if found_region is None:
                return make_response(False, [], 'Region not found')

            user_data = [{
                'company_id': company_id,
                'region_caption': found_region.caption,
                'region': found_region
            }]

        return make_response(True, user_data, 'Region found with id: {}'.format(found_region.id))

    @classmethod
    def get_regions_for_company(cls, company_id):
        with get_cloud_connection_safe() as conn_cloud:
            regions_query = select([region]).where(region.owner_id == int(company_id))
            found_regions = conn_cloud.execute(regions_query).fetchall()

            if found_regions is None:
                return make_response(False, [], 'Region not found')

            user_data = [dict(
                id=l.id,
                ext_id=l.external_id,
                name=l.caption,
                alive=l.alive
            ) for l in found_regions]

        return make_response(True, user_data, 'Found num regions: {}'.format(len(user_data)))

    @classmethod
    def export_region(cls, company_id):
        with get_cloud_connection_safe() as conn_cloud:

            assign_cluster = aliased(region)

            region_query = select([
                region.caption.label('region_name'),
                region.external_id.label('region_id'),
                assign_cluster.external_id.label('parent_region_id'),
                literal_column('1').label('region_action'),
            ]).where(and_(
                region.owner_id == company_id,
                region.alive.is_(True)
            )).select_from(
                outerjoin(region, assign_cluster,
                          assign_cluster.id == region.parent_id
            ))

            run_query = conn_cloud.execute(region_query)
            output_results = []

            for x in run_query:
                output_results.append(dict(zip(x.keys(), x.values())))


        return sorted(output_results, key=lambda name: name["region_name"])


class MachineTypeQueryOnCloud(object):

    @classmethod
    def get_machine_type_by_external_ids(cls, company_id, external_ids):
        with get_cloud_connection_safe() as conn_cloud:
            search_ids = external_ids if isinstance(external_ids, (list, tuple, set)) else [external_ids]
            regions_query = select([machine_type]).where(and_(
                machine_type.owner_id == int(company_id), machine_type.code.in_(search_ids),
                machine_type.alive.is_(True))
            )
            found_m_types = conn_cloud.execute(regions_query).fetchall()

            if found_m_types is None:
                return make_response(False, [], 'Machine types not found')

            user_data = [dict(
                id=mt.id,
                name=mt.caption,
                ext_id=mt.code,
                alive=mt.alive
            ) for mt in found_m_types]

        return make_response(True, user_data, 'Found machine types: {}'.format(len(user_data)))

    @classmethod
    def get_machine_types_for_company(cls, company_id):
        with get_cloud_connection_safe() as conn_cloud:
            types_query = select([machine_type]).where(machine_type.owner_id == int(company_id))
            found_types = conn_cloud.execute(types_query).fetchall()

            if found_types is None:
                return make_response(False, [], 'Machine Type not found')

            user_data = [dict(
                id=l.id,
                ext_id=l.code,
                name=l.caption,
                alive=l.alive,
                is_default=l.is_default
            ) for l in found_types]

        return make_response(True, user_data, 'Found num machine types: {}'.format(len(user_data)))

    @classmethod
    def export_machine_type(cls, company_id):
        with get_cloud_connection_safe() as conn_cloud:
            machine_type_query = select(
                [
                    machine_type.caption.label('machine_type_name'),
                    machine_type.code.label('machine_type_id'),
                    literal_column('1').label('machine_type_action'),
                ]
            ).where(
                and_(machine_type.owner_id == company_id,
                     machine_type.alive.is_(True)
                     )
            )

            run_query = conn_cloud.execute(machine_type_query)
            output_results = []

            for x in run_query:
                output_results.append(dict(zip(x.keys(), x.values())))

        return sorted(output_results, key=lambda name: name["machine_type_name"])


class ProductQueryOnCloud(object):
    @classmethod
    def get_products_for_company(cls, company_id):
        with get_cloud_connection_safe() as conn_cloud:
            products_query = select([product]).where(product.owner_id == int(company_id))
            found_products = conn_cloud.execute(products_query).fetchall()

            if found_products is None:
                return make_response(False, [], 'Product not found')
            user_data = [
                dict(
                    id=p.id,
                    ext_id=p.code,
                    name=p.caption,
                    alive=p.alive,
                    barcode=p.barcode,
                    barcode1=p.barcode_1,
                    barcode2=p.barcode_2,
                    barcode3=p.barcode_3,
                    barcode4=p.barcode_4,
                    is_composite=p.is_composite,
                    is_combo=p.is_combo,
                    use_packing=p.use_packing
                ) for p in found_products
            ]

        return make_response(
            True,
            user_data,
            'Found num products: {}'.format(len(user_data))
        )

    @classmethod
    def export_product(cls, company_id):
        with get_cloud_connection_safe() as conn_cloud:

            product_query = select([
                product.caption.label('product_name'),
                product.code.label('product_id'),
                literal_column('1').label('product_action'),
                product.price,
                tax_rate.value.label('tax_rate'),
                product_category.code.label('product_category_id'),
                product.barcode.label('default_barcode'),
                product.barcode_1.label('barcode1'),
                product.barcode_2.label('barcode2'),
                product.barcode_3.label('barcode3'),
                product.barcode_4.label('barcode4'),
                product.weight,
                product.use_packing,
                product.description,
                product.shelf_life.label('short_shelf_life'),
                product.minimum_allowed_age.label('age_verification'),
                product.quantity.label('capacity'),
                product.minimum_route_pickup,
                product.blacklisted
            ]).where(and_(
                product.owner_id == company_id,
                product.alive.is_(True),
                product.tax_rate_id == tax_rate.id
            )).select_from(
                join(product_category, product,
                          product.category_id == product_category.id)
            )

            # product query for product without category_id
            product_query_widouth = select([
                product.caption.label('product_name'),
                product.code.label('product_id'),
                literal_column('1').label('product_action'),
                product.price,
                tax_rate.value.label('tax_rate'),
                product.category_id.label('product_category_id'),
                product.barcode.label('default_barcode'),
                product.barcode_1.label('barcode1'),
                product.barcode_2.label('barcode2'),
                product.barcode_3.label('barcode3'),
                product.barcode_4.label('barcode4'),
                product.weight,
                product.use_packing,
                product.description,
                product.shelf_life.label('short_shelf_life'),
                product.minimum_allowed_age.label('age_verification'),
                product.quantity.label('capacity'),
                product.minimum_route_pickup,
                product.blacklisted
            ]).where(and_(
                product.owner_id == company_id,
                product.alive.is_(True),
                product.tax_rate_id == tax_rate.id
            ))

            run_query = conn_cloud.execute(product_query)
            output_results = []

            for x in run_query:
                dict_for_insert = dict(zip(x.keys(), x.values()))

                for k in dict_for_insert.keys():
                    # convert Decimals to string
                    if 'price' == k:
                        val = dict_for_insert.get(k)
                        val = str(val)
                        dict_for_insert[k] = val
                    if 'tax_rate' == k:
                        val = dict_for_insert.get(k)
                        val = str(val)
                        dict_for_insert[k] = val
                    if 'blacklisted' == k:
                        val = dict_for_insert.get(k)
                        val = 1 if val else 0
                        dict_for_insert[k] = val

                output_results.append(dict_for_insert)

            run_query_without_category_id = conn_cloud.execute(product_query_widouth)
            for x in run_query_without_category_id:
                dict_for_insert = dict(zip(x.keys(), x.values()))

                for k in dict_for_insert.keys():
                    if 'price' == k:
                        val = dict_for_insert.get(k)
                        val = str(val)
                        dict_for_insert[k] = val
                    if 'tax_rate' == k:
                        val = dict_for_insert.get(k)
                        val = str(val)
                        dict_for_insert[k] = val
                    if 'blacklisted' == k:
                        val = dict_for_insert.get(k)
                        val = 1 if val else 0
                        dict_for_insert[k] = val
                    if 'product_category_id' == k:
                        val = dict_for_insert.get(k)
                        if val in [None, '']:
                            val = str(' - ')
                            dict_for_insert[k] = val
                        else:
                            val = str('exclude')
                            dict_for_insert[k] = val
                if dict_for_insert not in output_results:
                    if dict_for_insert['product_category_id'] != 'exclude':
                        output_results.append(dict_for_insert)
        return sorted(output_results, key=lambda name: name['product_name'])

    @classmethod
    def get_tax_rates_for_company(cls, company_id):
        with get_cloud_connection_safe() as conn_cloud:
            tax_rate_query = select([
                tax_rate.value
            ]).where(and_(
                tax_rate.owner_id == company_id,
                tax_rate.alive.is_(True)
            ))

            run_query = conn_cloud.execute(tax_rate_query)
            output_results = []

            for x in run_query:
                dict_for_insert = dict(zip(x.keys(), x.values()))
                output_results.append(dict_for_insert)


            result = sorted(output_results, key=lambda x: x['value'])
            return result


    @classmethod
    def products_machine_status(cls, company_id, array_of_product_ids):
        if not array_of_product_ids:
            return {'allowed': True}

        with get_cloud_connection_safe() as conn_cloud:

            list_tuple = tuple(str(x) for x in array_of_product_ids)

            prod_exte = []

            q_products = (
                select([product.id, product.code, machine_column.machine_id]).
                    where(
                    and_(product.code.in_(list_tuple),
                         product.owner_id == company_id,
                         product.alive.is_(True))
                ).select_from(
                    join(machine_column, product,
                             and_(
                                 machine_column.product_id == product.id,
                                 machine_column.alive.is_(True),
                                 machine_column.pushed.is_(True),
                             or_(
                                 machine_column.product_id == product.id,
                                 machine_column.changed_price != machine_column.price
                             ),
                             or_(
                                 machine_column.product_id == product.id,
                                 machine_column.changed_price_2 != machine_column.price_2
                             ),
                             or_(
                                 machine_column.product_id == product.id,
                                 machine_column.alive.is_(True),
                                 machine_column.pushed.is_(False)
                             ),
                             or_(
                                 machine_column.product_id == product.id,
                                 machine_column.changed_index != None
                             )
                            )
                    )
                )
            )

            ex_query = conn_cloud.execute(q_products)
            if not ex_query.rowcount:
                return {'allowed': True}

            ext_data = ex_query.fetchall()

            # ====================================================

            mch_id = tuple(set([x.machine_id for x in ext_data]))

            for x in ext_data:
                prod_exte.append({
                    'machine_id': x[2],
                    'product_external_id': x[1]
                })

            q_machines = select([machine]).where(
                and_(machine.id.in_(mch_id),
                    machine.alive.is_(True),
                    )
            ).order_by(machine.id).limit(20)

            ex_machines = conn_cloud.execute(q_machines)

            def check_id(mch_id):
                for x in prod_exte:
                    if x['machine_id'] == mch_id:
                        return x['product_external_id']

            mvh_external_id = []
            for x in ex_machines:
                mvh_external_id.append({
                    'machine_external_id': x.external_id,
                    'product_external_id': check_id(x.id)
                })

            products_with_machines = {}
            for i in mvh_external_id:
                p_ext_id = i['product_external_id']
                machine_ext_id = i['machine_external_id']

                try:
                    products_with_machines[p_ext_id].append(machine_ext_id)
                except KeyError:
                    products_with_machines[p_ext_id] = []
                    products_with_machines[p_ext_id].append(machine_ext_id)


        return {'allowed': False, 'results': products_with_machines}


    @classmethod
    def products_planogram_status(cls, company_id, array_of_product_ids):
        if not array_of_product_ids:
            return {'allowed': True}

        with get_cloud_connection_safe() as conn_cloud:

            list_tuple = tuple(str(x) for x in array_of_product_ids)

            prod_exte = []

            q_products = (
                select([product.id, product.code, layout_column.layout_id]).
                    where(
                    and_(product.code.in_(list_tuple),
                         product.owner_id == company_id,
                         product.alive.is_(True))
                ).select_from(
                    join(
                        layout_column, product,
                        and_(
                            layout_column.product_id == product.id,
                            layout_column.alive.is_(True)
                        )
                    )
                )
            )

            ex_query = conn_cloud.execute(q_products)
            if not ex_query.rowcount:
                return {'allowed': True}

            ext_data = ex_query.fetchall()

            # ====================================================

            # product template ids
            pt_id = tuple(set([x.layout_id for x in ext_data]))

            for x in ext_data:
                prod_exte.append({
                    'product_template_id': x.layout_id,
                    'product_external_id': x.external_id
                })

            q_product_templates = select([product_template]).where(
                and_(product_template.id.in_(pt_id),
                    product_template.deleted == False,
                    product_template.alive == True)
            ).order_by(product_template.id).limit(20)

            ex_product_templates = conn_cloud.execute(q_product_templates)

            def check_id(pt_id):
                for x in prod_exte:
                    if x['product_template_id'] == pt_id:
                        return x['product_external_id']

            output_dict = []
            for x in ex_product_templates:
                output_dict.append({
                    'product_template_name': x.caption,
                    'product_external_id': check_id(x.id)
                })

            products_with_product_templates = {}
            for i in output_dict:
                p_ext_id = i['product_external_id']
                product_template_name = i['product_template_name']

                try:
                    products_with_product_templates[p_ext_id].append(product_template_name)
                except KeyError:
                    products_with_product_templates[p_ext_id] = []
                    products_with_product_templates[p_ext_id].append(product_template_name)


        return {'allowed': False, 'results': products_with_product_templates}


class PackingsQueryOnCloud(object):
    @classmethod
    def get_packing_names_for_company(cls, company_id):

        with get_cloud_connection_safe() as conn_cloud:

            packings_query = select([packing_name]).where(and_(packing_name.owner_id == int(company_id), packing_name.alive == True))
            found_packings = conn_cloud.execute(packings_query).fetchall()

            if found_packings is None:
                return make_response(False, [], 'Packing name not found')

            user_data = [dict(
                id=l.id,
                ext_id=l.external_id,
                packing_name=l.caption
            ) for l in found_packings]

        return make_response(True, user_data, 'Found num packing names: {}'.format(len(user_data)))

    @classmethod
    def get_product_packings_for_company(cls, company_id):

        with get_cloud_connection_safe() as conn_cloud:

            product_packing_size_query = select([product_packing_size, packing_name.caption.label('packing_name'), product.code.label('product_code'), packing_name.external_id.label('packing_name_ext_id')]).where(
                and_(product.owner_id == int(company_id),
                     product.alive == True,
                     )).select_from(
                outerjoin(product_packing_size, product, product.id == product_packing_size.product_id).
                outerjoin(packing_name, product_packing_size.packing_name_id == packing_name.id)).order_by(product_packing_size.product_id)

            found_packings_sizes = conn_cloud.execute(product_packing_size_query).fetchall()

            if found_packings_sizes is None:
                return make_response(False, [], 'Packing name not found')

            packing_data = []

            for packing in found_packings_sizes:

                packing_dict = dict(
                        id=packing.id,
                        ext_id=packing.external_id,
                        packing_name=packing.packing_name,
                        packing_name_id=packing.packing_name_ext_id,
                        name=packing.packing_name,
                        alive=packing.alive,
                        default=packing.default,
                        quantity=packing.quantity,
                        barcode=packing.barcode,
                        product_id=packing.product_code,
                        system_default=packing.system_default
                    )

                packing_data.append(packing_dict)

        return make_response(True, packing_data, 'Found num packing sizes: {}'.format(len(packing_data)))

    @classmethod
    def get_used_non_singlepack_packings(cls, company_id):

        with get_cloud_connection_safe() as conn_cloud:

            packings_query = select([product_packing_size.barcode, product_packing_size.external_id]).where(and_(product_packing_size.owner_id == int(company_id),
                                                                               product_packing_size.alive == True,
                                                                               product_packing_size.system_default == False))
            found_packings = conn_cloud.execute(packings_query).fetchall()

            if found_packings is None:
                return make_response(False, [], 'Packing name not found')

            user_data = [dict(
                barcode=l.barcode,
                ext_id=l.external_id
            ) for l in found_packings]

        return make_response(True, user_data, 'Found num packings: {}'.format(len(user_data)))

    @classmethod
    def export_packing(cls, company_id):

        with get_cloud_connection_safe() as conn_cloud:

            packing_query = select([
                packing_name.external_id.label('packing_name_id'),
                product_packing_size.external_id.label('packing_id'),
                product_packing_size.quantity,
                product_packing_size.barcode,
                product_packing_size.default,
                product.code.label('product_id'),
                literal_column('1').label('packing_action'),
            ]).where(and_(
                product.owner_id == company_id,
                product_packing_size.alive.is_(True),
                product_packing_size.enabled.is_(True)
            )).select_from(
                outerjoin(product_packing_size, packing_name, packing_name.id == product_packing_size.packing_name_id).
                outerjoin(product,  product.id == product_packing_size.product_id)
            )

            run_query = conn_cloud.execute(packing_query)
            output_results = []

            for x in run_query:
                output_results.append(dict(zip(x.keys(), x.values())))

        return sorted(output_results, key=lambda name: name["packing_id"])


class ClientQueryOnCloud(object):

    @classmethod
    def get_client_by_external_id(cls, company_id, external_id):

        with get_cloud_connection_safe() as conn_cloud:
            client_query = select([client]).where(
                (client.owner_id == int(company_id)) & (client.external_id == str(external_id)))
            found_client = conn_cloud.execute(client_query).fetchone()

            if found_client is None:
                return make_response(False, [], 'Client not found')

            user_data = [{
                'owner_id': company_id,
                'client_id': found_client.id,
                'parent_caption': found_client.caption,
                'parent_code': found_client.external_id
            }]

        return make_response(True, user_data, 'Client found with id: {}'.format(found_client.id))

    @classmethod
    def get_clients_for_company(cls, company_id):

        with get_cloud_connection_safe() as conn_cloud:

            clients_query = select([client]).where(client.owner_id == int(company_id))
            found_clients = conn_cloud.execute(clients_query).fetchall()

            if found_clients is None:
                return make_response(False, [], 'Clients not found')

            user_data = [dict(
                id=l.id,
                ext_id=l.external_id,
                name=l.caption,
                alive=l.alive
            ) for l in found_clients]

        return make_response(True, user_data, 'Found num clients: {}'.format(len(user_data)))

    @classmethod
    def get_clients_with_active_machines_for_company(cls, company_id):

        with get_cloud_connection_safe() as conn_cloud:

            clients_query = (select([client.id,
                                    client.external_id,
                                    client.alive,
                                    client.caption])
                                .where(and_(
                                    client.owner_id == int(company_id),
                                    machine.alive.is_(True)))
                                .select_from(
                                    outerjoin(client, machine,
                                              client.id == machine.client_id))
                                .distinct())

            found_clients = conn_cloud.execute(clients_query).fetchall()

            if found_clients is None:
                return make_response(False, [], 'Clients not found')

            user_data = [dict(
                id=l.id,
                caption=l.caption,
                ext_id=l.external_id,
                alive=l.alive
            ) for l in found_clients]

        return make_response(True, user_data, 'Found num clients: {}'.format(len(user_data)))

    @classmethod
    def export_client(cls, company_id):

        with get_cloud_connection_safe() as conn_cloud:

            parent_client = aliased(client)

            client_query = select([
                client.caption.label('client_name'),
                client.external_id.label('client_id'),
                parent_client.external_id.label('parent_client_id'),
                client_type.external_id.label('client_type_id'),
                literal_column('1').label('client_action'),
            ]).where(and_(
                client.owner_id == company_id,
                client.alive.is_(True)
            )).select_from(
                outerjoin(client, client_type, client_type.id == client.client_type_id)
                .outerjoin(parent_client, parent_client.id == client.parent_id)

            )

            run_query = conn_cloud.execute(client_query)
            output_results = []

            for x in run_query:
                output_results.append(dict(zip(x.keys(), x.values())))

        return sorted(output_results, key=lambda name: name["client_name"])


class ClientTypeQueryOnCloud(object):

    @classmethod
    def get_client_type_by_external_id(cls, company_id, external_id):

        with get_cloud_connection_safe() as conn_cloud:
            client_type_query = select([client_type]).where(
                (client_type.owner_id == int(company_id)) & (client_type.external_id == str(external_id)))
            found_client_type = conn_cloud.execute(client_type_query).fetchone()

            if found_client_type is None:
                return make_response(False, [], 'Client type not found')

            user_data = [{
                'owner_id': company_id,
                'client_type_id': found_client_type.id,
                'parent_caption': found_client_type.caption,
                'parent_code': found_client_type.code
            }]

        return make_response(True, user_data, 'Client type found with id: {}'.format(found_client_type.id))

    @classmethod
    def get_alive_clients_types_for_company(cls, company_id):

        with get_cloud_connection_safe() as conn_cloud:
            client_types_query = select([client_type]).where((client_type.owner_id == int(company_id)) & (client_type.alive == True))
            found_client_types = conn_cloud.execute(client_types_query).fetchall()

            if found_client_types is None:
                return make_response(False, [], 'Client types not found')

            user_data = [dict(
                id=l.id,
                ext_id=l.external_id,
                name=l.caption,
                alive=l.alive
            ) for l in found_client_types]

        return make_response(True, user_data, 'Found num client types: {}'.format(len(user_data)))


class DeviceQueryOnCloud(object):
    @classmethod
    def check_existing_device_on_cloud(cls, device_pid, company_id):
        with get_cloud_connection_safe() as conn_cloud:
            device_query = select([device]).where(and_(
                device.owner_id == int(company_id),
                device.pid == device_pid,
                device.active.is_(True)))

            device_result = conn_cloud.execute(device_query).fetchone()
            if device_result:
                row = dict(zip(device_result.keys(), device_result))
                result = [{
                   'device_pid': row.get('pid'),
                   'device_alive': row.get('alive'),
                   'device_status': row.get('connection_status'),
                   'device_type_id': row.get('device_type_id'),
                   'device_id': row.get('id'),
                }]

                if row.get('id'):
                    machine_query = select([device_machine]).where(
                        device_machine.device_id == row.get('id'),
                        )
                    machine_result = conn_cloud.execute(machine_query).fetchone()
                    machine_result_id = machine_result.machine_id
                    if machine_result_id:
                        machine_external_id_query = select([machine.external_id]).where(
                            machine.id == machine_result_id)
                        machine_external_id = conn_cloud.execute(machine_external_id_query).fetchone()
                        machine_row = dict(zip(machine_external_id.keys(), machine_external_id))
                        machine_external_id = machine_row.get('external_id')
                    else:
                        machine_external_id = None
                else:
                    machine_external_id = None

                machine_and_device_result = {
                    'device_result': result,
                    'machine_external_id': machine_external_id,
                    'machine_id': machine_result_id
                }
                return machine_and_device_result
            return False

    @classmethod
    def check_existing_machine_on_cloud(cls, machine_id, company_id):
        with get_cloud_connection_safe() as conn_cloud:
            machine_query = select([machine]).where(and_(
                machine.owner_id == int(company_id),
                machine.id == machine_id,
                machine.alive.is_(True))
            )

            machine_result = conn_cloud.execute(machine_query).fetchone()
            if machine_result:
                row = dict(zip(machine_result.keys(), machine_result))
                result = {
                   'machine_external_id': row.get('external_id'),
                   'machine_name': row.get('caption'),
                   'machine_id': machine_id,
                   'device_type': row.get('device_type')
                }

                return result
            return False


class VendQueryOnCloud(object):
    @classmethod
    def get_vends_by_transaction_ids(cls, company_id, transaction_ids):
        if len(transaction_ids)==0:
            return make_response(False, [], 'Vend not found')

        with get_cloud_connection_safe() as conn_cloud:

            search_ids = transaction_ids if isinstance(transaction_ids, (list, tuple, set)) else [transaction_ids]
            vends_query = (select([vends.id, vends.transaction_id, vends.data, machine.owner_id])
                            .where(and_(vends.transaction_id.in_(search_ids),
                                        machine.owner_id == company_id))
                            .select_from(
                            join(vends, machine,
                                 vends.machine_id == machine.id)
            )
                           )

            found_vends = conn_cloud.execute(vends_query).fetchall()

            if found_vends is None:
                return make_response(False, [], 'Vend not found')

            user_data = [dict(id=v.id, transaction_id=v.transaction_id, company_id=v.owner_id) for v in found_vends]

        return make_response(True, user_data, 'Found num vends: {}'.format(len(user_data)))


class PlanogramQueryOnCloud(object):
    @classmethod
    def get_planogram_for_company(cls, company_id):
        with get_cloud_connection_safe() as conn_cloud:
            planogram_query = select([product_templates]).where(
                product_templates.owner_id == int(company_id)
            )
            found_planogram = conn_cloud.execute(planogram_query).fetchall()

            if found_planogram is None:
                return make_response(False, [], 'planogram not found')

            data = [dict(
                id=p.id,
                ext_id=p.external_id,
                name=p.caption,
                alive=p.alive,
                enabled=p.enabled,
                product_warning_percentage=p.product_warning_percentage,
                component_warning_percentage=p.component_warning_percentage,
                mail_notification=p.mail_notification,

            ) for p in found_planogram]

        return make_response(True, data, 'Found num planogram: {}'.format(len(data)))

    @classmethod
    def get_recipe(cls, owner_id):
        with get_cloud_connection_safe() as conn_cloud:
            column_product = aliased(product)
            recipe_query = select([
                recipe,
                column_product.code.label('product_ext_id')
            ]).where(
                column_product.owner_id == int(owner_id)
            ).select_from(outerjoin(column_product, recipe, column_product.id == recipe.product_id))

            found_recipe = conn_cloud.execute(recipe_query).fetchall()

            if found_recipe is None:
                return make_response(False, [], 'recipe not found')

            data = [dict(
                id=r.id,
                name=r.caption,
                default=r.default,
                alive=r.alive,
                code=r.code,
                product_id=r.product_id,
                product_ext_id=r.product_ext_id
            ) for r in found_recipe]
        return make_response(True, data, 'Found num recipe: {}'.format(len(data)))

    @classmethod
    def get_product_component(cls, company_id):
        with get_cloud_connection_safe() as conn_cloud:
            product_component_query = select(
                [product_components, product_component_items.caption.label('product_components_caption')]).select_from(
                outerjoin(product_components, product_component_items,
                          product_components.component_id == product_component_items.id)
            ).where(and_(product_components.alive.is_(True),
                         product_component_items.owner_id == company_id)).order_by(product_component_items.caption)

            found_component = conn_cloud.execute(product_component_query).fetchall()

            if found_component is None:
                return make_response(False, [], 'product component not found')

            data = [dict(
                id=c.id,
                product_component_alive=c.alive,
                product_component_recipe_id=c.recipe_id,
                product_component_id=c.component_id,
                product_component_quantity=c.quantity,
                product_component_caption=c.product_components_caption,
            ) for c in found_component]
        return data

    @classmethod
    def get_layout_component(cls):
        with get_cloud_connection_safe() as conn_cloud:
            recipe_query = select([layout_components])

            found_component = conn_cloud.execute(recipe_query).fetchall()

            if found_component is None:
                return make_response(False, [], 'recipe not found')

            data = [dict(
                id=r.id,
                layout_id=r.layout_id,
                component_max_quantity=r.max_quantity,
                component_warning_quantity=r.warning_quantity,
                component_notify_warning=r.notify_warning,
                component_next_fill_quantity=r.next_fill_quantity,
                product_component_id=r.component_id,
                component_tags=r.tags,
                alive=r.alive,
            ) for r in found_component]
        return data

    @classmethod
    def get_layout_column_tags(cls):
        with get_cloud_connection_safe() as conn_cloud:
            client_query = select([
                tags.caption.label('caption'),
                tags.id.label('id'),
                tags.alive.label('alive'),
                layout_columns_tags.layoutcolumn_id.label('layoutcolumn_id'),
                layout_columns_tags.id.label('layout_columns_tags_id'),
            ]).select_from(
                outerjoin(layout_columns_tags, tags, layout_columns_tags.tags_id == tags.id)
            ).where(tags.alive.is_(True))

            run_query = conn_cloud.execute(client_query)
            data = [dict(
                id=r.id,
                alive=r.alive,
                caption=r.caption,
                column_id=r.layoutcolumn_id,
                columns_tags_id=r.layout_columns_tags_id,
            ) for r in run_query]
        return data

    @classmethod
    def get_combo_recipe(cls, owner_id):
        with get_cloud_connection_safe() as conn_cloud:
            column_product = aliased(product)
            recipe_query = select([
                combo_recipe,
                column_product.code.label('product_ext_id')
            ]).where(column_product.owner_id == int(owner_id)).select_from(
                outerjoin(column_product, combo_recipe, column_product.id == combo_recipe.product_id))

            found_recipe = conn_cloud.execute(recipe_query).fetchall()

            if found_recipe is None:
                return make_response(False, [], 'recipe not found')

            data = [dict(
                id=r.id,
                name=r.caption,
                code=r.code,
                alive=r.alive,
                product_ext_id=r.product_ext_id,
            ) for r in found_recipe]
        return make_response(True, data, 'Found num recipe: {}'.format(len(data)))

    @classmethod
    def get_columns_for_planogram(cls, planogram_id):
        with get_cloud_connection_safe() as conn_cloud:
            layout_column_query = select([layout_columns]).where(layout_columns.layout_id == int(planogram_id))
            found_columns = conn_cloud.execute(layout_column_query).fetchall()
            if found_columns is None:
                return make_response(False, [], 'planogram column not found')
            data = [dict(id=l.id, index=l.index, layout_id=l.layout_id, ) for l in found_columns]

        return data

    @classmethod
    def get_planogram_columns(cls, planogram_ext_ids, company_id):
        with get_cloud_connection_safe() as conn_cloud:

            client_query = select([
                product_templates.caption.label('planogram_name'),
                product_templates.external_id.label('external_id'),
                product_templates.product_warning_percentage.label('product_warning_percentage'),
                product_templates.component_warning_percentage.label('component_warning_percentage'),
                product_templates.mail_notification.label('mail_notification'),
                product_templates.pricelist_count.label('multiple_pricelists'),
                layout_columns.index.label('index'),
                layout_columns.price.label('price_1'),
                layout_columns.price_2.label('price_2'),
                layout_columns.price_3.label('price_3'),
                layout_columns.price_4.label('price_4'),
                layout_columns.price_5.label('price_5'),
                layout_columns.minimum_route_pickup.label('minimum_route_pickup'),
                layout_columns.alive.label('alive'),
                layout_columns.id.label('columns_id'),
                layout_columns.notify_warning.label('notify_warning'),
                layout_columns.warning_quantity.label('warning_quantity'),
                layout_columns.next_fill_quantity.label('fill_rate'),
                layout_columns.max_quantity.label('capacity'),
                product_templates.id.label('planogram_id'),

            ]).select_from(
                outerjoin(product_templates, layout_columns, product_templates.id == layout_columns.layout_id)
            ).where(and_(
                product_templates.owner_id == company_id,
                product_templates.external_id.in_(planogram_ext_ids),
                product_templates.alive.is_(True),
                layout_columns.alive.is_(True),
            ))

            run_query = conn_cloud.execute(client_query)
            data = [dict(
                caption=x.planogram_name,
                external_id=x.external_id,
                planogram_id=x.planogram_id,
                product_warning_percentage=x.product_warning_percentage,
                component_warning_percentage=x.component_warning_percentage,
                mail_notification=x.mail_notification,
                multiple_pricelists=x.multiple_pricelists,
                index=x.index,
                price=x.price_1,
                price_2=x.price_2,
                price_3=x.price_3,
                price_4=x.price_4,
                price_5=x.price_5,
                minimum_route_pickup=x.minimum_route_pickup,
                alive=x.alive,
                column_id=x.columns_id,
                notify_warning=x.notify_warning,
                warning_quantity=x.warning_quantity,
                next_fill_quantity=x.fill_rate,
                component_max_quantity=x.capacity,
                max_quantity=x.capacity)
                for x in run_query
            ]
        return data

    @classmethod
    def company_price_definition(cls, company_id):
        with get_cloud_connection_safe() as conn_cloud:
            company_query = select([company]).where(
                (company.id == company_id) & (company.enabled.is_(True)) & (company.alive.is_(True)))

            result = conn_cloud.execute(company_query)

            company_data = result.fetchone()
            if company_data is None:
                return False
            actual_company_price = company_data.extra_pricelists_setting
            actual_company_price_list = []
            if actual_company_price != 0:
                for x in range(1, actual_company_price+1):
                    actual_company_price_list.append('price_'+str(x))
            if len(actual_company_price_list):
                return actual_company_price_list
        return False

    @classmethod
    def company_product_rotation_access(cls, company_id):
        with get_cloud_connection_safe() as conn_cloud:
            company_query = select([company_access]).where(
                company.id == company_id).select_from(outerjoin(company, company_access, company.access_id==company_access.id))
            result = conn_cloud.execute(company_query)

            company_access_data = result.fetchone()
            if not company_access_data:
                return False

        return company_access_data.product_rotation_groups

    @classmethod
    def export_planogram(cls, company_id):
        company_price_list_definition = cls.company_price_definition(company_id)
        if not company_price_list_definition:
            company_price_list_definition = ['price_1', 'price_2']

        product_rotation_groups_access = cls.company_product_rotation_access(company_id)
        default_price_value = "%0.2f" % float(0)

        tags_captions = func.string_agg(tags.caption, literal_column("','"))
        output_results = []
        empty_value = PlanogramEnum.EMPTY_HEADER_VALUE.value
        with get_cloud_connection_safe() as conn_cloud:
            column_recipe = aliased(recipe)
            column_product = aliased(product)
            client_query = select([
                product_templates.caption.label('planogram_name'),
                product_templates.external_id.label('planogram_id'),
                product_templates.product_warning_percentage.label('product_warning_percentage'),
                product_templates.component_warning_percentage.label('component_warning_percentage'),
                product_templates.mail_notification.label('mail_notification'),
                product_templates.pricelist_count.label('multiple_pricelists'),
                layout_columns.index.label('column_number'),
                layout_columns.price.label('price_1'),
                layout_columns.price_2.label('price_2'),
                layout_columns.price_3.label('price_3'),
                layout_columns.price_4.label('price_4'),
                layout_columns.price_5.label('price_5'),
                layout_columns.notify_warning.label('warning'),
                layout_columns.warning_quantity.label('warning_quantity'),
                layout_columns.next_fill_quantity.label('fill_rate'),
                layout_columns.max_quantity.label('capacity'),
                layout_columns.minimum_route_pickup.label('minimum_route_pickup'),
                column_recipe.code.label('recipe_id'),
                column_product.code.label('product_id'),
                product_rotation_group.external_id.label('product_rotation_group_id'),
                tags_captions.label('tags'),
            ]).select_from(
                outerjoin(product_templates, layout_columns, product_templates.id == layout_columns.layout_id)
                .outerjoin(column_recipe, column_recipe.id == layout_columns.recipe_id)
                .outerjoin(column_product, column_product.id == layout_columns.product_id)
                .outerjoin(layout_columns_tags, layout_columns.id == layout_columns_tags.layoutcolumn_id)
                .outerjoin(tags, layout_columns_tags.tags_id == tags.id)
                .outerjoin(product_rotation_group, product_rotation_group.id == layout_columns.product_rotation_group_id)

            ).where(and_(
                product_templates.owner_id == company_id,
                product_templates.alive.is_(True),
                layout_columns.alive.is_(True),
            )).group_by(
                    product_templates.caption,
                    product_templates.external_id,
                    product_templates.product_warning_percentage,
                    product_templates.component_warning_percentage,
                    product_templates.mail_notification,
                    product_templates.pricelist_count,
                    layout_columns.index,
                    layout_columns.price,
                    layout_columns.price_2,
                    layout_columns.price_3,
                    layout_columns.price_4,
                    layout_columns.price_5,
                    layout_columns.notify_warning,
                    layout_columns.warning_quantity,
                    layout_columns.next_fill_quantity,
                    layout_columns.max_quantity,
                    layout_columns.minimum_route_pickup,
                    column_recipe.code,
                    column_product.code,
                    product_rotation_group.external_id
                )

            run_query = conn_cloud.execute(client_query)
            for x in run_query:
                row = (dict(zip(x.keys(), x.values())))
                warning = row.get('warning')
                pricelist_count = int(row.get('multiple_pricelists'))

                try:
                    price_1 = "%0.2f" % float(row.get('price_1')) if row.get('price_1') not in empty_value else default_price_value
                    price_2 = "%0.2f" % float(row.get('price_2')) if row.get('price_2') not in empty_value else default_price_value
                    price_3 = "%0.2f" % float(row.get('price_3')) if row.get('price_3') not in empty_value else default_price_value
                    price_4 = "%0.2f" % float(row.get('price_4')) if row.get('price_4') not in empty_value else default_price_value
                    price_5 = "%0.2f" % float(row.get('price_5')) if row.get('price_5') not in empty_value else default_price_value
                    del row['price_1']
                    del row['price_2']
                    del row['price_3']
                    del row['price_4']
                    del row['price_5']
                    for export_price in company_price_list_definition:
                        if export_price == 'price_1':
                            row[export_price] = price_1 if pricelist_count >= 1 else default_price_value
                        if export_price == 'price_2':
                            row[export_price] = price_2 if pricelist_count >= 2 else default_price_value
                        if export_price == 'price_3':
                            row[export_price] = price_3 if pricelist_count >= 3 else default_price_value
                        if export_price == 'price_4':
                            row[export_price] = price_4 if pricelist_count >= 4 else default_price_value
                        if export_price == 'price_5':
                            row[export_price] = price_5 if pricelist_count == 5 else default_price_value
                except Exception as e:
                    logger.error('Error on planogram multi price export, error: {}'.format(e))

                warning_quantity = int(row.get('warning_quantity'))
                mail_notification = 1 if row.get('mail_notification') else 0
                if warning_quantity > 0:
                    warning = warning_quantity
                elif warning_quantity == 0:
                    if warning:
                        warning = warning_quantity
                    else:
                        warning = ""

                layout_column_tag = (row.get('tags'))
                if layout_column_tag not in ["", None]:
                    right_layout_column_tag = re.sub("\s+", ",", layout_column_tag.strip())
                    del row['tags']
                    row['tags'] = right_layout_column_tag
                if layout_column_tag == '<null>':
                    del row['tags']
                    row['tags'] = None

                del row['mail_notification']
                del row['warning']
                del row['warning_quantity']
                row['planogram_action'] = 1
                row['mail_notification'] = mail_notification
                row['warning'] = warning
                output_results.append(row)

        planogram_name_sort = sorted(output_results, key=lambda k: (k['planogram_name'], k['column_number']))
        return planogram_name_sort, company_price_list_definition, product_rotation_groups_access


class UserQueryOnCloud(object):

    @classmethod
    def get_users(cls, company_id):

        with get_cloud_connection_safe() as conn_cloud:
            admin_roles = ('MulticompanyManager', 'SuperDistributor',
                           'SuperadminSupport', 'SuperAdministrator')
            user_query = select(
                [
                    user.id.label('id'),
                    custom_user.external_id.label('ext_id'),
                    custom_user.alive.label('alive'),
                    user.email.label('email'),
                    user_role.name.label('user_role')
                ]).\
                where((custom_user.company_id == int(company_id))).\
                select_from(join(user, custom_user, user.id == custom_user.user_ptr_id).
                            join(user_role, user_role.id == custom_user.user_role_id)).\
                where(~user_role.name.in_(admin_roles))
            users = conn_cloud.execute(user_query).fetchall()

            if users is None:
                return make_response(False, [], 'Users not found')

            user_data = [{
                'id': u.id,
                'ext_id': u.ext_id,
                'alive': u.alive,
                'email': u.email,
                'user_role': u.user_role
            } for u in users]

        return make_response(True, user_data, 'Found num users: {}'.format(len(user_data)))

    @classmethod
    def cast_bool(cls, value):
        return 'true' if value else 'false'

    @classmethod
    def export_user(cls, company_id):

        with get_cloud_connection_safe() as conn_cloud:
            user_query = select(
                [
                    custom_user.external_id.label('user_id'),
                    user.first_name.label('first_name'),
                    user.last_name.label('last_name'),
                    user.email.label('email'),
                    user_role.name.label('user_role'),
                    custom_user.timezone.label('timezone'),
                    custom_user.tel.label('phone'),
                    custom_user.preferred_language.label('language'),
                    custom_user.notify_email.label('service_email_notification'),
                    custom_user.notify_sms.label('service_sms_notification'),
                    custom_user.api_service.label('service_staff_mobile_app'),
                    custom_user.technician_view.label('service_staff_mobile_technical_view'),
                    user_role.filling_route_assign_view.label('assign_filling_route'),
                    user_role.event_assign_view.label('assign_event')
                ]).\
                where((custom_user.company_id == int(company_id)) &
                      custom_user.alive.is_(True) & user_role.alive.is_(True)).\
                select_from(join(user, custom_user, user.id == custom_user.user_ptr_id).
                            join(user_role, user_role.id == custom_user.user_role_id))
            users = conn_cloud.execute(user_query).fetchall()

            user_data = [{
                'user_id': u.user_id,
                'first_name': u.first_name,
                'last_name': u.last_name,
                'email': u.email,
                'user_role': u.user_role,
                'timezone': u.timezone,
                'phone': u.phone,
                'language': u.language,
                'service_email_notification': cls.cast_bool(u.service_email_notification),
                'service_sms_notification': cls.cast_bool(u.service_sms_notification),
                'service_staff_mobile_app': cls.cast_bool(u.service_staff_mobile_app),
                'service_staff_mobile_technical_view': cls.cast_bool(u.service_staff_mobile_technical_view),
                'assign_filling_route': cls.cast_bool(u.assign_filling_route),
                'assign_event': cls.cast_bool(u.assign_event)
            } for u in users]

            return sorted(user_data, key=lambda u: u["user_id"])


class ProductRotationGroupQueryOnCloud(object):

    @classmethod
    def get_product_rotation_groups_for_company(cls, company_id):
        with get_cloud_connection_safe() as conn_cloud:
            prg_query = (select([
                product_rotation_groups.id.label('id'),
                product_rotation_groups.external_id.label('external_id'),
                product_rotation_groups.name.label('name'),
                product_rotation_groups.alive.label('alive'),
                product_rotation_groups.enabled.label('enabled'),
                product.external_id.label('products'),
            ]).where(
                product_rotation_groups.owner_id == int(company_id))
            ).select_from(outerjoin(outerjoin(
                product_rotation_groups, product_rotation_assignments, product_rotation_groups.id == product_rotation_assignments.rotation_group_id),
                product, product_rotation_assignments.product_id == product.id))

            found_prg = conn_cloud.execute(prg_query).fetchall()

            if found_prg is None:
                return make_response(False, [], 'rotation groups not found')

            data = [dict(
                id=prg.id,
                ext_id=prg.external_id,
                name=prg.name,
                alive=prg.alive,
                enabled=prg.enabled,
                products=prg.products,
                prg_external_id=prg.external_id,

            ) for prg in found_prg]

        return make_response(True, data, 'Found product rotation groups: {}'.format(len(data)))

    @classmethod
    def get_products_for_specific_product_rotation_group(cls, prg_id):
        with get_cloud_connection_safe() as conn_cloud:

            prg_query = select([
                product_rotation_groups.name.label('prg_name'),
                product_rotation_groups.external_id.label('prg_ext_id'),
                product.caption.label('product_name'),
                product.external_id.label('product_ext_id'),
                product.code.label('product_code'),
            ]).where(and_(
                product_rotation_assignments.rotation_group_id == int(prg_id),
                product_rotation_assignments.alive.is_(True),
            )).select_from(
                outerjoin(product_rotation_assignments, product_rotation_groups,
                          product_rotation_assignments.rotation_group_id == product_rotation_groups.id)
                    .outerjoin(product, product.id == product_rotation_assignments.product_id)
            )

            query_results = conn_cloud.execute(prg_query).fetchall()

            if query_results is None:
                return make_response(False, [], 'product rotation groups not found')

            data = [dict(
                prg_name=prg.prg_name,
                prg_ext_id=prg.prg_ext_id,
                product_name=prg.product_name,
                product_ext_id=prg.product_ext_id,
                product_code=prg.product_code,

            ) for prg in query_results]

        return make_response(True, data, 'Found product rotation groups: {}'.format(len(data)))


class MachineCategoryQueryOnCloud(object):

    @classmethod
    def get_categories_for_company(cls, company_id):
        with get_cloud_connection_safe() as conn_cloud:
            categories_query = select([machine_category]).where(machine_category.owner_id==int(company_id))
            found_categories = conn_cloud.execute(categories_query).fetchall()

            if found_categories is None:
                return make_response(False, [], 'Machine Category not found')

            user_data = [dict(
                id=c.id,
                ext_id=c.external_id,
                name=c.caption,
                alive=c.alive
            ) for c in found_categories]

            return make_response(
                True,
                user_data,
                'Found num machine categories: {}'.format(len(user_data))
            )


class WarehouseQueryOnCloud(object):

    @classmethod
    def get_warehouses_for_company(cls, company_id):
        with get_cloud_connection_safe() as conn_cloud:
            warehouses_query = select([warehouse]).where(warehouse.owner_id==int(company_id))
            found_warehouses = conn_cloud.execute(warehouses_query).fetchall()

            if found_warehouses is None:
                return make_response(False, [], 'Warehouse not found')

            user_data = [dict(
                id=w.id,
                ext_id=w.external_id,
                name=w.caption,
                alive=w.alive
            ) for w in found_warehouses]

            return make_response(
                True,
                user_data,
                'Found num warehouses: {}'.format(len(user_data))
            )
