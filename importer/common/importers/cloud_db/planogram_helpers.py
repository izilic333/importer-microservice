import pandas as pd

from common.importers.cloud_db.mixin import handle_multi_price_on_planogram, prepare_planogram_data, \
    handle_planogram_column, handle_specific_planogram_fields
from common.mixin.enum_errors import EnumValidationMessage as Const, PlanogramEnum
from common.mixin.validation_const import ImportAction
from common.mixin.enum_errors import enum_message_on_specific_language
from common.logging.setup import logger
empty_header_value = PlanogramEnum.EMPTY_HEADER_VALUE.value


class PlanogramHandler(object):
    def __init__(self, all_company_planograms, all_company_product, planogram_name_and_external_id,
                 all_cloud_company_product_ext_id, products_data_frame_object, language):

        self.all_company_planograms = all_company_planograms
        self.all_company_product = all_company_product
        self.planogram_name_and_external_id = planogram_name_and_external_id
        self.all_cloud_company_product_ext_id = all_cloud_company_product_ext_id
        self.products_data_frame_object = products_data_frame_object
        self.language = language
        self.warnings = []

    def append_warning(self, record_part, message_part):
        self.warnings.append({
            'record': record_part,
            'message': message_part
        })

    def message_translator(self, const, *args):
        return enum_message_on_specific_language(const.value, self.language, *args)

    def check_exists_planogram_in_company(self, planogram_name, planogram_external_id):
        planogram_external_id_exists = False
        planogram_name_exists = False

        validation_check_status = {
            "search_results": [],
            "planogram_external_id_exists": False,
            "planogram_name_exists": False
        }

        # there is no planogram data in cloud company
        if not len(self.all_company_planograms):
            return validation_check_status

        if str(planogram_external_id) in self.planogram_name_and_external_id:
            planogram_external_id_exists = True
        if str(planogram_name) in self.planogram_name_and_external_id:
            planogram_name_exists = True

        search_results = list(filter(lambda x: x['name'] == planogram_name and x['ext_id'] == planogram_external_id, self.all_company_planograms))
        validation_check_status = {
            "search_results": search_results,
            "planogram_external_id_exists": planogram_external_id_exists,
            "planogram_name_exists": planogram_name_exists
        }

        return validation_check_status

    def check_exists_product_in_company(self, product_ext_id):
        if product_ext_id in self.all_cloud_company_product_ext_id:
            return True
        return False

    def composite_and_combo_product_check(self, product_ext_id):
        composite_product = False
        combo_product = False
        if product_ext_id in self.all_cloud_company_product_ext_id:
            result = self.products_data_frame_object.loc[
                self.products_data_frame_object['ext_id'] == str(product_ext_id)].to_dict('records')
            for item in result:
                if item['is_composite']:
                    composite_product = True
                if item['is_combo']:
                    combo_product = True

        return composite_product, combo_product

    @staticmethod
    def check_product_column_on_planogram(import_column, all_planogram_cloud_columns):
        for x in all_planogram_cloud_columns:
            if str(x['index']) == str(import_column):
                return True
        return False

    @staticmethod
    def check_column_per_planogram(planogram_import_data):
        """
        This method search duplicate planogram_name with planogram column_number for import planogram data.
        :param planogram_import_data:
        :return: planogram_name & column_number repeat status
        """
        data_frame_object = pd.DataFrame(planogram_import_data, columns=['column_number', 'planogram_name',
                                                                         'planogram_id'])
        duplicate_column = data_frame_object[data_frame_object.duplicated()].to_dict('records')
        return duplicate_column

    @staticmethod
    def check_planogram_name_per_external_id(planogram_import_data):
        """
        This method search duplicate planogram_name with planogram external_id for import planogram data.
        :param planogram_import_data: data from planogram import file
        :return: planogram_name repeat status
        """
        errors_names = {}
        used_external_ids = set()
        used_planogram_name = []

        data_frame_object = pd.DataFrame(planogram_import_data, columns=['planogram_name', 'planogram_id'])
        data_frame_object = data_frame_object.groupby(['planogram_id'])['planogram_name'].agg(
            lambda grp: list(set(list(grp)))).to_dict()

        for name, unique_ids_per_name in sorted(data_frame_object.items()):
            if len(unique_ids_per_name) != 1:
                errors_names[name] = list(unique_ids_per_name)
                used_planogram_name.append(name)
            for check_id in unique_ids_per_name:
                if check_id in used_external_ids:
                    errors_names[name] = check_id
                    used_planogram_name.append(name)
            used_external_ids.update(list(unique_ids_per_name))
        return errors_names

    @staticmethod
    def check_planogram_external_id_per_planogram_name(planogram_import_data):
        """
        This method search duplicate planogram_external_id with planogram_name for import planogram data.
        :param planogram_import_data: data from planogram import file
        :return: planogram_external_id repeat dict
        """
        errors_names = {}
        used_external_ids = set()
        used_planogram_name = []

        data_frame_object = pd.DataFrame(planogram_import_data, columns=['planogram_name', 'planogram_id'])
        data_frame_object = data_frame_object.groupby(['planogram_name'])['planogram_id'].agg(
            lambda grp: list(set(list(grp)))).to_dict()

        for name, unique_ids_per_name in sorted(data_frame_object.items()):
            if len(unique_ids_per_name) != 1:
                errors_names[name] = list(unique_ids_per_name)
                used_planogram_name.append(name)
            for check_id in unique_ids_per_name:
                if check_id in used_external_ids:
                    errors_names[name] = check_id
                    used_planogram_name.append(name)
            used_external_ids.update(list(unique_ids_per_name))
        return errors_names

    def planogram_name_and_external_id_processor(self, planogram_column_repeat, planogram_name_repeat,
                                                 planogram_ext_id_repeat):
        remove_planogram_name_column_repeat = []
        remove_planogram_name_ext_id_repeat = []
        remove_planogram_ext_id_repeat = []

        for item_element in planogram_column_repeat:
            self.append_warning(item_element['planogram_id'], self.message_translator(
                Const.PLANOGRAM_IMPORT_COLUMN_REPEAT, item_element['planogram_name'], item_element['column_number']))
            if item_element['planogram_name'] not in remove_planogram_name_column_repeat:
                remove_planogram_name_column_repeat.append(item_element['planogram_name'])

        for planogram_ext_id, planogram_name in planogram_name_repeat.items():
            if type(planogram_name) is list:
                remove_planogram_name_ext_id_repeat = remove_planogram_name_ext_id_repeat+planogram_name
                for pl_name in planogram_name:
                    self.append_warning(planogram_ext_id, self.message_translator(
                        Const.PLANOGRAM_NAME_IMPORT_REPEAT, pl_name, planogram_ext_id))
            else:
                remove_planogram_name_ext_id_repeat.append(planogram_name)
                self.append_warning(planogram_ext_id, self.message_translator(
                    Const.PLANOGRAM_NAME_IMPORT_REPEAT, planogram_name, planogram_ext_id))

        for planogram_name, planogram_ext_id in planogram_ext_id_repeat.items():
            if type(planogram_ext_id) is list:
                remove_planogram_ext_id_repeat = remove_planogram_ext_id_repeat+planogram_ext_id
                for pl_ext_id in planogram_ext_id:
                    self.append_warning(pl_ext_id, self.message_translator(
                        Const.PLANOGRAM_EXTERNAL_ID_IMPORT_REPEAT, pl_ext_id, planogram_name))
            else:
                self.append_warning(planogram_ext_id, self.message_translator(
                    Const.PLANOGRAM_EXTERNAL_ID_IMPORT_REPEAT, planogram_ext_id, planogram_name))
                remove_planogram_ext_id_repeat.append(planogram_ext_id)

        remove_planogram_name_column_repeat = list(set(remove_planogram_name_column_repeat))
        remove_planogram_name_ext_id_repeat = list(set(remove_planogram_name_ext_id_repeat))
        remove_planogram_ext_id_repeat = list(set(remove_planogram_ext_id_repeat))

        processor_results = {
            'remove_planogram_name_column_repeat': remove_planogram_name_column_repeat,
            'remove_planogram_name_ext_id_repeat': remove_planogram_name_ext_id_repeat,
            'remove_planogram_ext_id_repeat': remove_planogram_ext_id_repeat
        }

        return processor_results, self.warnings


class PlanogramValidation(object):
    def __init__(self, work_on, empty_header, language, alive_products, company_id,
                 pr_rotation_group_data_frame, recipe_data):
        self.work_on = work_on
        self.empty_header = empty_header
        self.language = language
        self.alive_products = alive_products
        self.company_id = company_id
        self.recipe_data = recipe_data
        self.pr_rotation_group_data_frame = pr_rotation_group_data_frame
        self.max_column = PlanogramEnum.MAX_COLUMNS.value

    @staticmethod
    def message_structure(record_part, message_part):

        message = {
            'record': record_part,
            'message': message_part
        }
        return message

    def message_translator(self, const, *args):
        return enum_message_on_specific_language(const.value, self.language, *args)

    def validate_planogram_multi_price(self, company_prices, multi_price, import_item):

        check_price = []
        price_column_for_message = []
        defined_import_multi_price = []
        company_multi_price = ['price_2', 'price_3', 'price_4', 'price_5']
        warnings = []
        errors = []
        removed_import_row = []

        for x in range(1, multi_price + 1):
            defined_import_multi_price.append('price_' + str(x))

        # discard file if import multiple price list is bigger than defined price on company
        if not company_prices and multi_price > 2:

            errors.append(self.message_structure(import_item['planogram_id'], self.message_translator(
                Const.PLANOGRAM_PRICE_LIST, multi_price)))
            errors.append(self.message_structure(self.work_on, self.message_translator(Const.ALL_ENTITIES_REMOVED)))
            return errors, warnings, removed_import_row

        # discard file if multi price is greater than company price
        if company_prices:
            if multi_price > len(company_prices):
                errors.append(self.message_structure(
                    import_item['planogram_id'],
                    self.message_translator(Const.PLANOGRAM_COMPANY_PRICE_CHECK, multi_price, len(company_prices))
                ))
                errors.append(self.message_structure(self.work_on, self.message_translator(Const.ALL_ENTITIES_REMOVED)))
                return errors, warnings, removed_import_row

            # discard row if multi price is les than sent column and this column has some value
            elif multi_price < len(company_prices):
                import_price_list_column = [x for x, y in import_item.items() if x.startswith('price_')]
                for y in range(1, multi_price + 1):
                    import_multi_price = 'price_' + str(y)
                    if import_multi_price not in check_price:
                        check_price.append(import_multi_price)

                if len(import_price_list_column) > len(check_price):
                    for price_column in import_price_list_column:
                        if price_column not in check_price and import_item[price_column]:
                            if price_column not in price_column_for_message:
                                price_column_for_message.append(price_column)

                if len(price_column_for_message):
                    for price in price_column_for_message:
                        warnings.append(self.message_structure(
                            import_item['planogram_id'],
                            self.message_translator(
                                Const.PLANOGRAM_IMPORT_PRICE_WRONG_PRICE_VALUE,
                                import_item['planogram_id'], price, multi_price)
                        ))

                        removed_import_row.append(import_item)
                        return errors, warnings, removed_import_row

        # check defined import price and if some price is not sent that price has same value as price_1
        price_1 = import_item['price_1']
        sent_import_multi_price = ['price_' + str(item) for item in range(2, len(defined_import_multi_price) + 1)]

        for price in sent_import_multi_price:
            if price in company_multi_price:
                import_price = import_item[price]
                if not import_price:
                    warnings.append(self.message_structure(
                        import_item['planogram_id'],
                        self.message_translator(Const.PLANOGRAM_IMPORT_PRICE_VALUE, import_item['planogram_id'], price)
                    ))
                    import_item[price] = price_1
                elif import_price == self.empty_header:
                    errors.append(self.message_structure(
                        import_item['planogram_id'],
                        self.message_translator(Const.PLANOGRAM_IMPORT_DISCARD_FILE_WRONG_COLUMN_SENT, multi_price, price)
                    ))
                    errors.append(
                        self.message_structure(self.work_on, self.message_translator(Const.ALL_ENTITIES_REMOVED)))

        return errors, warnings, removed_import_row

    def validate_planogram_product(
            self,
            product_existing_on_company_status,
            composite_product,
            combo_product,
            recipe_data_code,
            combo_recipe_data_code,
            pr_rotation_all_alive,
            import_item,
    ):

        planogram_ext_id = import_item['planogram_id']
        product_ext_id = import_item['product_id']
        recipe_ext_id = import_item['recipe_id']
        planogram_name = import_item['planogram_name']
        product_rotation_group_ext_id = import_item['product_rotation_group_id']
        warnings = []
        errors = []
        removed_import_row = []
        prg_id = ""
        if product_ext_id and product_ext_id != self.empty_header:
            if not product_existing_on_company_status:
                warnings.append(self.message_structure(planogram_ext_id, self.message_translator(
                    Const.PLANOGRAM_PRODUCT_NOT_FOUND, product_ext_id)))
                removed_import_row.append(import_item)
                return errors, warnings, removed_import_row, prg_id, import_item

            if not len(self.alive_products):
                warnings.append(self.message_structure(planogram_ext_id, self.message_translator(
                    Const.PLANOGRAM_PRODUCT_EMPTY, self.company_id)))
                removed_import_row.append(import_item)
                return errors, warnings, removed_import_row, prg_id, import_item

            product_match = list(filter(lambda item: item['ext_id'] == product_ext_id and item['alive'], self.alive_products))

            if len(product_match) > 1:
                warnings.append(self.message_structure(planogram_ext_id, self.message_translator(
                    Const.PLANOGRAM_PRODUCT_WITH_DUPLICATE_EXT_ID, planogram_name, product_ext_id)))
                removed_import_row.append(import_item)
                return errors, warnings, removed_import_row, prg_id, import_item

            recipe_match = list(filter(lambda item: item['code'] == recipe_ext_id and item['product_ext_id'] == product_ext_id and item['alive'], self.recipe_data))

            if recipe_match:
                if len(recipe_match) > 1:
                    warnings.append(self.message_structure(planogram_ext_id, self.message_translator(
                        Const.PLANOGRAM_PRODUCT_WITH_DUPLICATE_RECIPE_EXT_ID, planogram_name, recipe_ext_id)))
                    removed_import_row.append(import_item)
                    return errors, warnings, removed_import_row, prg_id, import_item

                found_recipe = recipe_match[0]['code']
                if found_recipe != '':
                    import_item['recipe_id'] = recipe_match[0]['id']

            if product_match:
                import_item['company_product_id'] = product_match[0]['id']
            else:
                import_item['company_product_id'] = ''

            # handle composite product
            not_defined_value = ["", None]
            if composite_product:
                import_item['is_composite'] = True
                import_item['is_combo'] = False
                recipe_ext_ids = [x['recipe_code'] for x in recipe_data_code]
                recipe_product_ext_ids = list(filter(lambda recipe: str(recipe['recipe_code']) == recipe_ext_id and recipe['product_ext_id'] == product_ext_id and recipe['alive'], recipe_data_code))
                recipe_product_ext_ids = [x['product_ext_id'] for x in recipe_product_ext_ids]

                if not len(recipe_data_code):
                    warnings.append(self.message_structure(planogram_ext_id, self.message_translator(
                        Const.PLANOGRAM_IMPORT_RECIPE_NOT_FOUND, planogram_name, recipe_ext_id)))
                    removed_import_row.append(import_item)
                    return errors, warnings, removed_import_row, prg_id, import_item

                elif recipe_ext_id in not_defined_value:
                    warnings.append(self.message_structure(planogram_ext_id, self.message_translator(
                        Const.PLANOGRAM_IMPORT_RECIPE_NOT_SENT, planogram_name, recipe_ext_id,
                        product_ext_id)))
                    removed_import_row.append(import_item)
                    return errors, warnings, removed_import_row, prg_id, import_item

                elif recipe_ext_id not in recipe_ext_ids:
                    warnings.append(self.message_structure(planogram_ext_id, self.message_translator(
                        Const.PLANOGRAM_IMPORT_RECIPE_NOT_FOUND, planogram_name, recipe_ext_id)))
                    removed_import_row.append(import_item)

                elif product_ext_id not in recipe_product_ext_ids:
                    warnings.append(self.message_structure(planogram_ext_id, self.message_translator(
                        Const.PLANOGRAM_IMPORT_RECIPE_NOT_PAIRED_WITH_PRODUCT, planogram_name, recipe_ext_id, product_ext_id)))
                    removed_import_row.append(import_item)

            # handle combo product
            elif combo_product:
                import_item['is_composite'] = False
                import_item['is_combo'] = True
                combo_recipe_ext_ids = [x['combo_recipe_code'] for x in combo_recipe_data_code]
                combo_recipe_product_ext_ids = list(filter(lambda recipe: str(recipe['combo_recipe_code']) == recipe_ext_id and recipe['product_ext_id'] == product_ext_id and recipe['alive'], combo_recipe_data_code))

                if len(combo_recipe_product_ext_ids) > 1:
                    warnings.append(self.message_structure(planogram_ext_id, self.message_translator(
                        Const.PLANOGRAM_PRODUCT_WITH_DUPLICATE_COMBO_RECIPE_EXT_ID, planogram_name, recipe_ext_id)))
                    removed_import_row.append(import_item)
                    return errors, warnings, removed_import_row, prg_id, import_item

                if combo_recipe_product_ext_ids:
                    import_item['combo_recipe_id'] = combo_recipe_product_ext_ids[0]["combo_recipe_id"]

                combo_recipe_product_ext_ids = [x['product_ext_id'] for x in combo_recipe_product_ext_ids]

                if not len(combo_recipe_data_code):
                    warnings.append(self.message_structure(planogram_ext_id, self.message_translator(
                        Const.PLANOGRAM_IMPORT_COMBO_RECIPE_NOT_FOUND, planogram_name, recipe_ext_id)))
                    removed_import_row.append(import_item)
                    return errors, warnings, removed_import_row, prg_id, import_item

                elif recipe_ext_id in not_defined_value:
                    warnings.append(self.message_structure(planogram_ext_id, self.message_translator(
                        Const.PLANOGRAM_IMPORT_COMBO_RECIPE_NOT_SENT, planogram_name, recipe_ext_id,
                        product_ext_id)))
                    removed_import_row.append(import_item)
                    return errors, warnings, removed_import_row, prg_id, import_item
                elif recipe_ext_id not in combo_recipe_ext_ids:
                    warnings.append(self.message_structure(planogram_ext_id, self.message_translator(
                        Const.PLANOGRAM_IMPORT_COMBO_RECIPE_NOT_FOUND, planogram_name, recipe_ext_id)))
                    removed_import_row.append(import_item)
                    return errors, warnings, removed_import_row, prg_id, import_item

                elif product_ext_id not in combo_recipe_product_ext_ids:
                    warnings.append(self.message_structure(planogram_ext_id, self.message_translator(
                        Const.PLANOGRAM_IMPORT_COMBO_RECIPE_NOT_PAIRED_WITH_PRODUCT, planogram_name, recipe_ext_id, product_ext_id)))
                    removed_import_row.append(import_item)

            # handle regular product
            else:
                import_item['is_composite'] = False
                import_item['is_combo'] = False
                if recipe_ext_id and recipe_ext_id != self.empty_header:
                    warnings.append(self.message_structure(
                        planogram_ext_id,
                        self.message_translator(
                            Const.PLANOGRAM_IMPORT_REGULAR_PRODUCT_WITH_RECIPE, product_ext_id,
                            recipe_ext_id, import_item['column_number'])))

        elif product_rotation_group_ext_id and product_rotation_group_ext_id != self.empty_header:
            product_rotation_with_assigned_product = list(filter(
                lambda planogram: planogram['ext_id'] == product_rotation_group_ext_id,
                self.pr_rotation_group_data_frame))

            if product_rotation_group_ext_id not in pr_rotation_all_alive:
                warnings.append(self.message_structure(planogram_ext_id, self.message_translator(
                    Const.PLANOGRAM_IMPORT_ROTATION_GROUP_NOT_FOUND_ON_CLOUD, product_rotation_group_ext_id)))
                removed_import_row.append(import_item)

            if product_rotation_with_assigned_product:
                prg_id = product_rotation_with_assigned_product[0]['id']
            assigned_product = []
            for prg in product_rotation_with_assigned_product:
                products = prg['products']
                assigned_product.append(products)
            if not assigned_product or None in assigned_product:
                warnings.append(self.message_structure(planogram_ext_id, self.message_translator(
                    Const.PLANOGRAM_ROTATION_GROUP_WITHOUT_PRODUCTS, product_rotation_group_ext_id)))
                removed_import_row.append(import_item)

        return errors, warnings, removed_import_row, prg_id, import_item

    def validate_planogram_import_action(self, action, alive_ids, all_alive_cloud_planogram_ids, column_check,
                                         planogram_match_result, planogram_ext_id):
        errors = []

        if action == ImportAction.UPDATE:
            if planogram_ext_id not in alive_ids:
                errors.append(self.message_structure(planogram_ext_id, self.message_translator(
                    Const.DATABASE_NOT_FOUND, self.work_on, planogram_ext_id, action.name)))
        elif action == ImportAction.DELETE:
            if planogram_ext_id not in all_alive_cloud_planogram_ids:
                errors.append(self.message_structure(planogram_ext_id, self.message_translator(
                    Const.DATABASE_NOT_FOUND, self.work_on, planogram_ext_id, action.name)))

        elif action == ImportAction.CREATE:
            if planogram_ext_id in all_alive_cloud_planogram_ids:
                if planogram_match_result and column_check:
                    errors.append(self.message_structure(planogram_ext_id, self.message_translator(
                        Const.DATABASE_FOUND, self.work_on, planogram_ext_id, action.name)))

        return errors

    def planogram_init_validation(self, planogram_name_column_repeat, planogram_name_ext_id_repeat,
                                  planogram_ext_id_repeat, import_item):
        warnings = []
        removed_import_row = []

        planogram_ext_id = import_item['planogram_id']
        product_ext_id = import_item['product_id']
        capacity = import_item['capacity']
        planogram_name = import_item['planogram_name']
        column = import_item['column_number']
        product_rotation_group_ext_id = import_item['product_rotation_group_id']
        fill_rate = import_item['fill_rate']
        planogram_warning = 0
        minimum_route_pickup = import_item['minimum_route_pickup'] if import_item['minimum_route_pickup'] else 0
        import_item['combo_recipe_id'] = ""
        if import_item['warning'] != self.empty_header:
            planogram_warning = int(import_item['warning']) if import_item['warning'] else 0

        try:
            column = int(import_item['column_number'])
        except Exception:
            warnings.append(self.message_structure(planogram_ext_id, self.message_translator(
                Const.PLANOGRAM_COLUMN_ERROR, planogram_name, column)))
            removed_import_row.append(import_item)
            return removed_import_row, warnings

        if column > self.max_column:
            warnings.append(self.message_structure(planogram_ext_id, self.message_translator(
                Const.PLANOGRAM_IMPORT_COLUMN_NUMBER_LIMIT, planogram_name, column)))
            removed_import_row.append(import_item)
            return removed_import_row, warnings

        if product_ext_id and product_ext_id != self.empty_header and product_rotation_group_ext_id \
                and product_rotation_group_ext_id != self.empty_header:
            warnings.append(self.message_structure(planogram_ext_id, self.message_translator(
                Const.PRODUCT_AND_ROTATION_GROUP_BOTH_PRESENT, column)))
            removed_import_row.append(import_item)
            return removed_import_row, warnings

        try:
            fill_rate = int(fill_rate)
        except Exception:
            warnings.append(self.message_structure(planogram_ext_id, self.message_translator(
                Const.PLANOGRAM_IMPORT_FILL_RATE, planogram_name, fill_rate)))
            removed_import_row.append(import_item)
            return removed_import_row, warnings

        if fill_rate > int(capacity):
            warnings.append(self.message_structure(planogram_ext_id, self.message_translator(
                Const.PLANOGRAM_IMPORT_FILL_RATE_ERROR, capacity, fill_rate)))
            removed_import_row.append(import_item)
            return removed_import_row, warnings

        if planogram_warning:
            if planogram_warning > int(capacity):
                warnings.append(self.message_structure(planogram_ext_id, self.message_translator(
                    Const.PLANOGRAM_WARNING_FIELD_ERROR, planogram_warning, capacity)))
                removed_import_row.append(import_item)
                return removed_import_row, warnings

        if planogram_name in planogram_name_column_repeat:
            removed_import_row.append(import_item)
            return removed_import_row, warnings

        if planogram_name in planogram_name_ext_id_repeat:
            removed_import_row.append(import_item)
            return removed_import_row, warnings

        if planogram_ext_id in planogram_ext_id_repeat:
            removed_import_row.append(import_item)

        if minimum_route_pickup != self.empty_header:
            try:
                if minimum_route_pickup:
                    minimum_route_pickup = int(minimum_route_pickup)
            except Exception:
                warnings.append(self.message_structure(planogram_ext_id, self.message_translator(
                    Const.PLANOGRAM_MINIMUM_ROUTE_PICKUP_POSITIVE_NUMBER, minimum_route_pickup)))
                removed_import_row.append(import_item)
                return removed_import_row, warnings

            if minimum_route_pickup and minimum_route_pickup  != self.empty_header:
                if minimum_route_pickup > fill_rate:
                    warnings.append(self.message_structure(planogram_ext_id, self.message_translator(
                        Const.PLANOGRAM_MINIMUM_ROUTE_PICKUP, minimum_route_pickup, fill_rate)))
                    removed_import_row.append(import_item)
                    return removed_import_row, warnings

                if minimum_route_pickup < 0:
                    warnings.append(self.message_structure(planogram_ext_id, self.message_translator(
                        Const.PLANOGRAM_MINIMUM_ROUTE_PICKUP_POSITIVE_NUMBER, minimum_route_pickup)))
                    removed_import_row.append(import_item)
                    return removed_import_row, warnings

        return removed_import_row, warnings


class PlanogramEntityDataBuilder(object):
    def __init__(self, import_data, planogram_name_delete):
        self.planogram_id = import_data['planogram_id']
        self.index = int(import_data['column_number'])
        self.notify_warning = import_data['notify_warning']
        self.recipe_id = import_data['recipe_id']
        self.planogram_ext_id = import_data['planogram_id']
        self.planogram_name = import_data['planogram_name']
        self.warning = import_data['warning']
        self.capacity = import_data['capacity']
        self.import_action = import_data['planogram_action']
        self.fill_rate = import_data['fill_rate']
        self.planogram_name_delete = planogram_name_delete
        self.mail_notification = import_data['mail_notification']
        self.component_warning_percentage = import_data['component_warning_percentage']
        self.product_warning_percentage = import_data['product_warning_percentage']
        self.tags = import_data.get('tags') if not import_data.get('tags') in empty_header_value else ''
        self.is_combo = import_data.get('is_combo', False)
        self.product_id = import_data.get('company_product_id', '')
        self.product_rotation_group_id = import_data['prg_id']
        self.is_composite = import_data.get('is_composite', False)
        self.multiple_pricelists = import_data['multiple_pricelists']
        self.combo_recipe_id = import_data['combo_recipe_id']
        self.price_1 = import_data['price_1']
        self.import_item = import_data
        self.minimum_route_pickup = import_data.get('minimum_route_pickup', 0)

    def layout_component_builder_for_update(self, layout_components, filtered_planogram_id):
        """
        This method build component for update on specific planogram column!
        :param layout_components: list of planogram components
        :param filtered_planogram_id: already exist planogram_id status
        :return: list of planogram components for update
        """
        component_for_update = []
        component_check = []

        if filtered_planogram_id:
            # Filter old layout_component on planogram
            old_component_match_data = list(
                filter(lambda c: c['layout_id'] == filtered_planogram_id and c['alive'], layout_components)
            )

            if old_component_match_data:
                for y in old_component_match_data:
                    component_id = y['id']
                    component_test_string = str(component_id) + str(self.planogram_ext_id)
                    if component_test_string not in component_check:
                        component_check.append(component_test_string)
                        y['component_action'] = ImportAction.DELETE.value
                        y['external_id'] = self.planogram_ext_id
                        y['component_id'] = component_id
                        y['caption'] = self.planogram_name
                        y['recipe_id'] = self.recipe_id
                        y['product_id'] = self.product_id
                        y['component_tags'] = '{}'
                        y['planogram_id'] = filtered_planogram_id
                        component_for_update.append(y)
        return component_for_update

    def layout_component_builder_for_insert(self, component_for_update, product_components, filtered_planogram_id):
        """
        This method build component for insert on specific planogram column!
        :param component_for_update: list of already match component for specific planogram column
        :param product_components: list of product components for specific company
        :return: list of planogram components for insert
        """
        component_for_insert = []
        components = list(filter(lambda c: c['product_component_recipe_id'] == int(self.recipe_id)
                                           and c['product_component_alive'], product_components))

        if self.planogram_name not in self.planogram_name_delete:
            component_action = 0
            for x in components:
                product_component_id = x['product_component_id']
                component_id = None
                if filtered_planogram_id:
                    planogram_component = list(filter(
                        lambda c: c['external_id'] == self.planogram_ext_id and
                                  c['product_component_id'] == product_component_id, component_for_update))
                    if planogram_component:
                        component_action = 1
                        component_id = planogram_component[0]['component_id']

                component_data_structure = {
                    'component_id': component_id,
                    'product_id': self.product_id,
                    'recipe_id': self.recipe_id,
                    'caption': self.planogram_name,
                    'product_component_id': product_component_id,
                    'planogram_id': filtered_planogram_id,
                    'external_id': self.planogram_ext_id,
                    'component_action': component_action,
                    "component_warning_quantity": self.warning,
                    "component_max_quantity": int(self.capacity),
                    "component_next_fill_quantity": self.fill_rate,
                    "component_notify_warning": self.notify_warning,
                    "component_tags": "{}",  # business logic rule, empty tags on components!
                }

                component_for_insert.append(component_data_structure)
        return component_for_insert

    def column_builder_for_update(self, repeat_planogram, layout_columns_tags, planogram_match_data):
        """
        This method build columns for update on specific planogram!
        :param repeat_planogram: (status if sent planogram with more than one columns) boolean
        :param layout_columns_tags: layout_columns_tags for specific planogram
        :return: list of planogram columns for insert
        """
        column_update_test_list = []
        column_update = []
        for pl_data in planogram_match_data:
            column_action = 1
            tags_action = 1
            if repeat_planogram or not self.product_rotation_group_id or self.import_action == ImportAction.DELETE.value:
                column_action = ImportAction.DELETE.value

            pl_data['column_action'] = column_action
            pl_data['recipe_id'] = self.recipe_id
            pl_data['minimum_route_pickup'] = self.minimum_route_pickup
            pl_data['product_rotation_group_id'] = self.product_rotation_group_id
            pl_data['product_id'] = self.product_id
            pl_data['combo_recipe_id'] = self.combo_recipe_id
            column_tags_match_data = list(
                filter(lambda c: c['column_id'] == pl_data['column_id'] and c['alive'], layout_columns_tags))
            if column_tags_match_data:
                tags_data = column_tags_match_data[0]
                pl_data['tags_id'] = tags_data['id']
                pl_data['tags_action'] = tags_action
                pl_data['columns_tags_id'] = tags_data['columns_tags_id']
                pl_data['column_id'] = tags_data['column_id']
                pl_data['tags_caption'] = tags_data['caption']

            column_test_string = str(pl_data['caption']) + str(pl_data['index']) + str(pl_data['external_id'])
            if column_test_string not in column_update_test_list:
                column_update_test_list.append(column_test_string)
                column_update.append(pl_data)

        return column_update

    def column_builder_for_insert(self, column_update, filtered_planogram_id):

        result = list(filter(lambda p: p['index'] == self.index and p['caption'] == self.planogram_name, column_update))
        tags_id = None
        column_id = None
        columns_tags_id = None
        tags_action = 0
        column_action = 0
        tags_for_insert = {}

        if result:
            column_action = 1
            tags_action = 1
            column_data_match = result[0]
            column_id = column_data_match['column_id']
            tags_id = column_data_match.get('tags_id', '')
            columns_tags_id = column_data_match.get('columns_tags_id', '')

        if self.planogram_name in self.planogram_name_delete:
            column_action = ImportAction.DELETE.value

        if self.is_composite:
            self.product_warning_percentage = 0
        else:
            self.component_warning_percentage = 0

        planogram_for_insert = {
            'tags_caption': self.tags,
            'is_combo': self.is_combo,
            'product_id': self.product_id,
            'is_composite': self.is_composite,
            'pricelist_count': self.multiple_pricelists,
            'product_rotation_group_id': self.product_rotation_group_id,
            'minimum_route_pickup': self.minimum_route_pickup,
            'caption': self.planogram_name,
            'external_id': self.planogram_ext_id,
            'planogram_id': filtered_planogram_id,
            'index': self.index,
            'max_quantity': self.capacity,
            'next_fill_quantity': self.fill_rate,
            'warning_quantity': self.warning,
            'recipe_id': self.recipe_id,
            'notify_warning': self.notify_warning,
            'alarm_quantity': '',
            'combo_recipe_id': self.combo_recipe_id,
            'import_action': self.import_action,
            'mail_notification': self.mail_notification,
            'component_warning_percentage': self.component_warning_percentage,
            'product_warning_percentage': self.product_warning_percentage,
            'column_action': column_action,
            'column_id': column_id,
            'tags_action': tags_action,
            'columns_tags_id': columns_tags_id,
            'tags_id': tags_id
        }

        if self.tags and self.planogram_name not in self.planogram_name_delete:
            tags_for_insert = {
                'external_id': self.planogram_ext_id,
                'planogram_id': filtered_planogram_id,
                'index': self.index,
                'import_action': self.import_action,
                "column_action": column_action,
                "column_id": column_id,
                "tags_action": tags_action,
                "columns_tags_id": columns_tags_id,
                "tags_caption": self.tags,
                'tags_id': tags_id
            }

        column_for_insert = handle_multi_price_on_planogram(
            planogram_for_insert, self.index, self.import_item)

        if self.import_action == ImportAction.DELETE.value:
            search_planogram_column = list(filter(lambda c: c['index'] == self.index and c['alive'], column_update))
            if not search_planogram_column:
                column_for_insert = {}

        return planogram_for_insert, column_for_insert, tags_for_insert


def planogram_processor(data, planograms_columns, product_components, layout_components, layout_columns_tags):
    validated_pl_name = list(map(lambda y: y['planogram_name'], data))
    validated_pl_ext_id = list(map(lambda y: y['planogram_id'], data))
    repeat_name_status = set(list(pd.Series(validated_pl_name)[pd.Series(validated_pl_name).duplicated()].values))
    repeat_external_id_status = set(
        list(pd.Series(validated_pl_ext_id)[pd.Series(validated_pl_ext_id).duplicated()].values))

    column_update = []
    component_update = []

    column_check = []
    planogram_check = []
    component_check = []

    planogram_for_database = []
    column_for_database = []
    component_for_database = []
    tags_for_database = []

    column_update_test_list = []
    planogram_name_delete = []
    planogram_working_data = []

    planogram_id = None

    for import_item in data:
        repeat_planogram = False
        planogram_ext_id = str(import_item['planogram_id'])
        planogram_name = str(import_item['planogram_name'])
        is_composite = import_item.get('is_composite')
        import_action = import_item.get('planogram_action')
        recipe_id = import_item.get('recipe_id')
        if import_action == ImportAction.UPDATE.value:
            if len(repeat_name_status) >= 1 and len(repeat_external_id_status) >= 1:
                repeat_planogram = True

        if import_action == ImportAction.DELETE.value and planogram_name not in planogram_name_delete:
            planogram_name_delete.append(planogram_name)

        import_field = handle_specific_planogram_fields(
            import_item['product_warning_percentage'],
            import_item['component_warning_percentage'],
            import_item.get('mail_notification', 0),
            import_item.get('warning'),
            import_item.get('fill_rate', 0),
            import_item.get('minimum_route_pickup', 0))

        import_item['product_warning_percentage'] = import_field['product_warning_percentage']
        import_item['component_warning_percentage'] = import_field['component_warning_percentage']
        import_item['mail_notification'] = import_field['mail_notification']
        import_item['warning'] = import_field['warning']
        import_item['fill_rate'] = import_field['fill_rate']
        import_item['notify_warning'] = import_field['notify_warning']
        import_item['minimum_route_pickup'] = import_field['minimum_route_pickup']
        specific_planogram_unique = planogram_name+str("_")+str(planogram_ext_id)
        data_entity_builder = PlanogramEntityDataBuilder(import_item, planogram_name_delete)

        try:

            if specific_planogram_unique not in planogram_check:
                planogram_check.append(specific_planogram_unique)
                planogram_match_data = list(filter(
                    lambda c: (c['external_id'] == planogram_ext_id or c['caption'] == planogram_name) and c['alive'],
                    planograms_columns))

                # handle planogram columns for update
                if planogram_match_data:
                    planogram_id = planogram_match_data[0]['planogram_id']
                    update_col = data_entity_builder.column_builder_for_update(
                        repeat_planogram, layout_columns_tags, planogram_match_data)
                    for x in update_col:
                        column_test_string = str(x['caption']) + str(x['index']) + str(x['external_id'])
                        if column_test_string not in column_update_test_list:
                            column_update_test_list.append(column_test_string)
                            column_update.append(x)
                else:
                    planogram_id = None

            # handle composite product & component on planogram
            if recipe_id and is_composite:
                update_comp = data_entity_builder.layout_component_builder_for_update(layout_components, planogram_id)
                insert_comp = data_entity_builder.layout_component_builder_for_insert(
                    update_comp, product_components, planogram_id)
                component_update = component_update + update_comp

                for x in insert_comp:
                    component_test_string = str(
                        x['component_id']) + str(
                        x['external_id']) + str(
                        x['product_component_id']
                    )
                    if component_test_string not in component_check:
                        component_check.append(component_test_string)
                        component_for_database.append(x)

            # handle planogram, planogram columns & tags for insert
            planogram_data, column_for_insert, tags_for_insert = data_entity_builder.column_builder_for_insert(
                column_update, planogram_id
            )
            if tags_for_insert:
                tags_for_database.append(tags_for_insert)

            if column_for_insert:
                planogram_index_test_string = str(
                    column_for_insert['caption']) + str(
                    column_for_insert['index']) + str(
                    column_for_insert['external_id']
                )
                if planogram_index_test_string not in column_check:
                    column_check.append(planogram_index_test_string)
                    column_for_database.append(column_for_insert)

            if specific_planogram_unique not in planogram_working_data:
                planogram_working_data.append(specific_planogram_unique)
                planogram_for_database.append(planogram_data)

        except Exception as e:
            logger.error(Const.PLANOGRAM_ERROR_BUILD_ENTITY_FOR_DATABASE.value.format(import_item, e))
            continue

    column_for_import = handle_planogram_column(column_for_database)

    planogram_data_structure = {
        'column_for_import': column_for_import,
        'column_for_update': column_update,
        'column_check': column_check,
        'component_for_import': component_for_database,
        'component_for_update': component_update,
        'check_component': component_check,
        'planogram_for_import': planogram_for_database,
        'planogram_for_delete': planogram_name_delete,
    }
    planogram_for_import, column_for_import, component_for_import = prepare_planogram_data(planogram_data_structure)
    return planogram_for_import, column_for_import, component_for_import, tags_for_database

