from decimal import Decimal
from common.mixin.enum_errors import PlanogramEnum
from common.mixin.validation_const import ImportAction

empty_header_value = PlanogramEnum.EMPTY_HEADER_VALUE.value


def handle_planogram_column(planograms):
    """
    This method handle recipe on layout_columns, because we have combo_recipe & regular recipe, so planogram importer
    handle this recipe based on product type!
    :param planograms: list of planogram dicts
    :return: list of planogram dicts updated with correct recipe
    """

    final_column = []
    check_column_list = []
    for x in planograms:
        column_check = str(x['index'])+str('_')+str('caption')
        if column_check not in check_column_list:
            final_column.append(x)

    for x in final_column:
        if x['is_composite']:
            recipe_id_value = x['recipe_id']
        else:
            recipe_id_value = ''
        if x['is_combo']:
            combo_recipe_id_value = x['combo_recipe_id']
        else:
            combo_recipe_id_value = ''

        if not ['planogram_id'] and x['column_action'] not in [ImportAction.UPDATE.value, ImportAction.DELETE.value]:
            planogram_id = {'planogram_id': x['external_id']}
            x.update({"planogram_id": planogram_id})

        x.update({"recipe_id": recipe_id_value})
        x.update({"combo_recipe_id": combo_recipe_id_value})

    return final_column


def handle_multi_price_on_planogram(column, index, import_item):
    """
    Handle multi price on planogram, set all non import price_list to price_1 & update import column...
    :param column: layout_column (list of layout_column dicts)
    :param index: specific import column number (int)
    :param import_item:  planogram import rows from file
    :return: layout_columns list of dicts with correct multi price
    """

    decimal_places_as_decimal = Decimal('0.01')
    price_1 = import_item['price_1']
    price_1 = Decimal(price_1).quantize(decimal_places_as_decimal)
    price_list_definition = ['price', 'price_2', 'price_3', 'price_4', 'price_5']
    # a) set import price_list (note: import_price_list max len is 5)
    # (price list is dynamically generated, in import file user can send only price_1, etc ...)
    import_price_list = [{k: v} for k, v in import_item.items() if k.startswith("price")]
    sent_price_list = []
    for prices in import_price_list:
        for key, value in prices.items():
            key = 'price' if key == "price_1" else key
            if key in price_list_definition:
                planogram_price = value
                if not planogram_price:
                    column[key] = price_1
                    continue
                column['index'] = index
                if planogram_price not in empty_header_value:
                    planogram_price = planogram_price.replace(',', '.')
                    planogram_price = Decimal(planogram_price).quantize(decimal_places_as_decimal)
                    column[key] = planogram_price
                else:
                    column[key] = None
            sent_price_list.append(key)

    # b) set all non import price_list to price_1
    for price in price_list_definition:
        if price not in sent_price_list:
            column['index'] = index
            column[price] = price_1
    return column


def handle_specific_case_planogram_entity_action(columns, components, planogram_name):
    """
    This method handle import action for layout_columns & layout_components import entity!
    :param columns: layout_columns (list of dicts)
    :param components: layout_components (list of dicts)
    :param planogram_name: import planogram name (str)
    :return: layout_columns & layout_components (list of dicts)
    """
    search_planogram_column = list(filter(lambda c: c['caption'] == planogram_name, columns))

    search_planogram_component = list(filter(lambda c: c['caption'] == planogram_name, components))

    for x in search_planogram_column:
        x['column_action'] = ImportAction.DELETE.value

    for x in search_planogram_component:
        x['component_action'] = ImportAction.DELETE.value

    return columns, components


def handle_specific_planogram_fields(product_warning_percentage, component_warning_percentage,
                                     mail_notification, warning, fill_rate, minimum_route_pickup):
    """
    This method convert specific import fields in to right values!
    :return: dict of import values for specific fields
    """

    if product_warning_percentage in empty_header_value:
        product_warning_percentage = 0
    else:
        product_warning_percentage = int(product_warning_percentage)

    if component_warning_percentage in empty_header_value:
        component_warning_percentage = 0
    else:
        component_warning_percentage = int(component_warning_percentage)

    if mail_notification not in empty_header_value:
        mail_notification = True if int(mail_notification) == 1 else False
    else:
        mail_notification = False

    try:
        if minimum_route_pickup in empty_header_value:
            minimum_route_pickup = 0
        else:
            minimum_route_pickup = int(minimum_route_pickup)
    except Exception:
        minimum_route_pickup = 0

    if warning in empty_header_value:
        warning = 0
        notify_warning = False
    else:
        try:
            warning = int(float(warning))
        except Exception:
            notify_warning = False
        else:
            if int(warning) > -1:
                notify_warning = True
            else:
                notify_warning = False

    if fill_rate not in empty_header_value:
        try:
            fill_rate = int(fill_rate)
        except Exception:
            fill_rate = 0

    data = {
        'product_warning_percentage': product_warning_percentage,
        'component_warning_percentage': component_warning_percentage,
        'mail_notification': mail_notification,
        'warning': warning,
        'notify_warning': notify_warning,
        'fill_rate': fill_rate,
        'minimum_route_pickup': minimum_route_pickup
    }

    return data


def prepare_planogram_data(working_data):
    """
    This method handle final import action for product_templates, layout_columns & layout_components & build final
    import entity.
    :param working_data: dict of already created import entity data structure!
    :return: final import action for product_templates, layout_columns & layout_components
    """

    # planogram data structure
    planogram_for_import = working_data['planogram_for_import']
    planogram_for_delete = working_data['planogram_for_delete']

    # column data structure
    column_for_update = working_data['column_for_update']
    column_for_import = working_data['column_for_import']

    # component data structure
    component_for_update = working_data['component_for_update']
    component_for_import = working_data['component_for_import']

    # column & component working list
    column_check = working_data['column_check']
    component_check = working_data['check_component']

    for x in column_for_update:
        test_column = str(x['caption'])+str(x['index'])+str(x['external_id'])
        if test_column not in column_check:
            column_check.append(test_column)
            column_for_import.append(x)

    for x in component_for_update:
        test_component = str(x['component_id']) + str(x['external_id']) + str(x['product_component_id'])
        if test_component not in component_check:
            component_check.append(test_component)
            component_for_import.append(x)

    for x in planogram_for_import:
        if x['caption'] in planogram_for_delete:
            x['import_action'] = ImportAction.DELETE.value

    # planogram delete action, all column and component for this planogram must be deleted!
    for planogram_name in planogram_for_delete:
        column_for_import, component_for_import = handle_specific_case_planogram_entity_action(
            column_for_import, component_for_import, planogram_name)

    return planogram_for_import, column_for_import, component_for_import


